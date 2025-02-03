import importlib
import json
from typing import Any, Dict, List, Optional


def dynamic_import(fully_qualified_name: str):
    """
    Import a class given its fully qualified name, e.g., "my_module.AddOneNode".
    Returns the class object.
    """
    module_name, class_name = fully_qualified_name.rsplit('.', 1)
    module = importlib.import_module(module_name)
    cls = getattr(module, class_name)
    return cls

class Node:
    """
    A composable transformation: context -> context.
    
    - name: Optional name for debugging
    - params: a dict of constructor parameters (JSON-serializable)
    - sub_nodes: child nodes if this node is a "composite"
    - node_type: a fully qualified Python class path (for dynamic import)
    
    By default, this Node does:
      1) If sub_nodes exist, run them in sequence on the context.
      2) Otherwise, do nothing (identity pass).
    Subclasses override __call__ to implement custom logic.
    """
    
    def __init__(
        self,
        name: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        sub_nodes: Optional[List["Node"]] = None,
        node_type: Optional[str] = None,
    ):
        self.name = name if name else self.__class__.__name__
        self.params = params or {}
        self.sub_nodes = sub_nodes or []
        
        if node_type is None:
            cls = self.__class__
            self.node_type = f"{cls.__module__}.{cls.__name__}"
        else:
            self.node_type = node_type

    def __call__(self, context: Any) -> Any:
        """
        Default behavior: if sub_nodes exist, run them in order;
        otherwise, return context unchanged.
        """
        if self.sub_nodes:
            current_ctx = context
            for node in self.sub_nodes:
                current_ctx = node(current_ctx)
            return current_ctx
        else:
            return context  # identity

    def __rshift__(self, other: "Node") -> "Node":
        """
        Compose self >> other: returns a CompositeNode that runs
        'self' then 'other' in sequence.
        """
        return CompositeNode(
            name=f"({self.name} >> {other.name})",
            sub_nodes=[self, other]
        )

    def to_json(self) -> Dict[str, Any]:
        """
        Serialize to a JSON-friendly dict. 
        """
        return {
            "node_type": self.node_type,
            "name": self.name,
            "params": self.params,
            "sub_nodes": [child.to_json() for child in self.sub_nodes],
        }

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "Node":
        """
        Deserialize a Node from a JSON dict by:
          1. dynamic_import(node_type)
          2. constructing that class with the stored 'params' and 'sub_nodes'
        """
        node_type = data["node_type"]
        node_cls = dynamic_import(node_type)  # load the actual class

        sub_nodes_data = data.get("sub_nodes", [])
        child_nodes = [cls.from_json(sd) for sd in sub_nodes_data]

        name = data["name"]
        params = data["params"]

        # Instantiate
        node_obj = node_cls(
            name=name,
            params=params,
            sub_nodes=child_nodes,
            node_type=node_type  # keep the same type string
        )
        return node_obj


class CompositeNode(Node):
    """
    A Node that runs its sub_nodes in sequence.
    """
    def __call__(self, context: Any) -> Any:
        print(f"[CompositeNode {self.name}] Starting sequence.")
        current_ctx = context
        for i, node in enumerate(self.sub_nodes, 1):
            print(f"[CompositeNode {self.name}] => Sub-node {i} ({node.name})")
            current_ctx = node(current_ctx)
        print(f"[CompositeNode {self.name}] Sequence result: {current_ctx}")
        return current_ctx

class AddOneNode(Node):
    """
    A Node that adds 'amount' to context["value"].
    """
    def __init__(
        self,
        name: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        sub_nodes: Optional[List[Node]] = None,
        node_type: Optional[str] = None
    ):
        super().__init__(name, params, sub_nodes, node_type)
        self.amount = self.params.get("amount", 1)

    def __call__(self, context: Dict[str, Any]) -> Dict[str, Any]:
        new_ctx = dict(context)
        old_val = new_ctx.get("value", 0)
        new_ctx["value"] = old_val + self.amount
        print(f"[AddOneNode {self.name}] {old_val} + {self.amount} -> {new_ctx['value']}")
        return new_ctx


class MultiplyNode(Node):
    """
    A Node that multiplies context["value"] by a factor.
    """
    def __init__(
        self,
        name: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        sub_nodes: Optional[List[Node]] = None,
        node_type: Optional[str] = None
    ):
        super().__init__(name, params, sub_nodes, node_type)
        self.factor = self.params.get("factor", 2)

    def __call__(self, context: Dict[str, Any]) -> Dict[str, Any]:
        new_ctx = dict(context)
        old_val = new_ctx.get("value", 1)
        new_ctx["value"] = old_val * self.factor
        print(f"[MultiplyNode {self.name}] {old_val} * {self.factor} -> {new_ctx['value']}")
        return new_ctx

class AskUserNode(Node):
    """
    A Node that pauses execution to ask the user a question, collects the answer,
    and stores it in the context. This simulates a 'tool call' to clarify details
    with a human user.
    
    Example param:
      "params": {
        "question": "Do you want to continue? (yes/no)"
        "target_key": "user_response"
      }
    """
    
    def __init__(
        self,
        name: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        sub_nodes: Optional[List[Node]] = None,
        node_type: Optional[str] = None
    ):
        super().__init__(name, params, sub_nodes, node_type)
        self.question = self.params.get("question", "Your input?")
        self.target_key = self.params.get("target_key", "user_response")

    def __call__(self, context: Dict[str, Any]) -> Dict[str, Any]:
        # We "call a tool" here. In a real system, this might be a Slack message,
        # a web form, or an external service. For simplicity, we use input().
        print(f"[AskUserNode {self.name}] Asking user: {self.question}")
        user_reply = input(f"{self.question} ")  # CLI prompt
        new_ctx = dict(context)
        new_ctx[self.target_key] = user_reply
        print(f"[AskUserNode {self.name}] Received '{user_reply}' and stored in '{self.target_key}'.")
        return new_ctx

if __name__ == "__main__":
    # 1. Build a pipeline that:
    #    - Adds 5,
    #    - Asks the user if they want to double,
    #    - If so, multiplies by 2
    #
    # For demonstration, the "if so" is not automatically enforced here,
    # but you could extend this to do branching logic. Right now, we'll
    # just record user input and run both nodes unconditionally.

    add_node = AddOneNode(
        name="AddFive",
        params={"amount": 5}
    )
    ask_node = AskUserNode(
        name="AskUser",
        params={
            "question": "Do you want to double the value? (yes/no)",
            "target_key": "user_decision"
        }
    )
    multiply_node = MultiplyNode(
        name="DoubleValue",
        params={"factor": 2}
    )
    
    # Compose them in a pipeline
    pipeline = add_node >> ask_node >> multiply_node

    # 2. Run it on an initial context
    initial_context = {"value": 10}
    print("\n--- Running Pipeline with User Clarification ---")
    final_context = pipeline(initial_context)
    print(f"Pipeline finished. Final context: {final_context}")

    # 3. Serialize to JSON
    pipeline_data = pipeline.to_json()
    pipeline_json = json.dumps(pipeline_data, indent=2)
    print("\n--- Pipeline Serialized to JSON ---")
    print(pipeline_json)

    # 4. Deserialize
    restored_data = json.loads(pipeline_json)
    restored_pipeline = Node.from_json(restored_data)

    # 5. Run again (will ask the user again)
    print("\n--- Running Restored Pipeline ---")
    second_context = {"value": 100}
    second_final = restored_pipeline(second_context)
    print(f"Restored pipeline finished. Final context: {second_final}")
