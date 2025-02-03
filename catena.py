"""
Example DSL with dynamic import, parameterized Nodes, and JSON serialization.

Basic Usage:
  1. Define specialized Node subclasses (e.g., AddOneNode) that store parameters.
  2. Use nodeA >> nodeB to create a composite node.
  3. Serialize to JSON, then deserialize to reconstruct the full pipeline.
"""

import importlib
import json
from typing import Any, Dict, List, Optional


##############################################################################
# Helper for Dynamic Import
##############################################################################

def dynamic_import(fully_qualified_name: str):
    """
    Import a class given its fully qualified name, e.g., "my_module.AddOneNode".
    Returns the class object.
    """
    module_name, class_name = fully_qualified_name.rsplit('.', 1)
    module = importlib.import_module(module_name)
    cls = getattr(module, class_name)
    return cls


##############################################################################
# Base Node Class
##############################################################################

class Node:
    """
    A composable transformation: context -> context.
    
    - name: Optional name for debugging
    - params: a dict of constructor parameters (JSON-serializable)
    - sub_nodes: child nodes if this node is a "composite"
    - node_type: a fully qualified Python class path (for dynamic import)
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
        
        # By default, store the *current* class's fully qualified name.
        # A subclass can override or set this differently if needed.
        if node_type is None:
            cls = self.__class__
            self.node_type = f"{cls.__module__}.{cls.__name__}"
        else:
            self.node_type = node_type

    def __call__(self, context: Any) -> Any:
        """
        Default behavior: If we have sub_nodes, run them in sequence.
        Otherwise, do nothing (identity).
        
        Subclasses will override this to provide real transformations.
        """
        if self.sub_nodes:
            current_ctx = context
            for node in self.sub_nodes:
                current_ctx = node(current_ctx)
            return current_ctx
        else:
            # Identity pass (override in child classes to do actual transformations).
            return context

    def __rshift__(self, other: "Node") -> "Node":
        """
        Compose self >> other: returns a Node that runs 'self' then 'other'.
        We do this by building a 'CompositeNode' or by directly storing sub_nodes.
        """
        return CompositeNode(
            name=f"({self.name} >> {other.name})",
            sub_nodes=[self, other]
        )

    def to_json(self) -> Dict[str, Any]:
        """
        Serialize the Node to a JSON-friendly dict.
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
        Deserialize a Node from a JSON-friendly dict by:
          1. Importing the 'node_type'
          2. Instantiating with the stored 'params'
          3. Recursively building 'sub_nodes'
        """
        node_type = data["node_type"]
        NodeClass = dynamic_import(node_type)  # load the actual class

        sub_nodes_data = data.get("sub_nodes", [])
        child_nodes = [cls.from_json(sd) for sd in sub_nodes_data]

        name = data["name"]
        params = data["params"]

        # We instantiate NodeClass with the same signature used in the constructor
        node_obj = NodeClass(
            name=name,
            params=params,
            sub_nodes=child_nodes,
            node_type=node_type  # keep the same type string
        )
        return node_obj


##############################################################################
# Composite Node
##############################################################################

class CompositeNode(Node):
    """
    A Node that runs its sub_nodes in sequence. 
    For demonstration, we override __call__ to show logs or custom logic.
    """
    def __call__(self, context: Any) -> Any:
        print(f"[CompositeNode {self.name}] Starting composition.")
        current_ctx = context
        for i, node in enumerate(self.sub_nodes, 1):
            print(f"[CompositeNode {self.name}] => Sub-node {i} ({node.name})")
            current_ctx = node(current_ctx)
        print(f"[CompositeNode {self.name}] Final result: {current_ctx}")
        return current_ctx


##############################################################################
# Example: AddOneNode
##############################################################################

class AddOneNode(Node):
    """
    A Node that adds 'amount' to context["value"].
    
    Instead of storing a function pointer, we store a parameter "amount"
    that we can easily serialize. 
    """

    def __init__(
        self,
        name: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        sub_nodes: Optional[List[Node]] = None,
        node_type: Optional[str] = None
    ):
        # We expect params to contain "amount"
        super().__init__(name, params, sub_nodes, node_type)
        # Our own property for convenience
        self.amount = params.get("amount", 1) if params else 1

    def __call__(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Adds self.amount to context["value"].
        """
        new_ctx = dict(context)
        old_val = new_ctx.get("value", 0)
        new_ctx["value"] = old_val + self.amount
        print(f"[AddOneNode {self.name}] {old_val} + {self.amount} -> {new_ctx['value']}")
        return new_ctx


##############################################################################
# Example: MultiplyNode
##############################################################################

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
        self.factor = params.get("factor", 2) if params else 2

    def __call__(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Multiplies context["value"] by self.factor.
        """
        new_ctx = dict(context)
        old_val = new_ctx.get("value", 1)
        new_ctx["value"] = old_val * self.factor
        print(f"[MultiplyNode {self.name}] {old_val} * {self.factor} -> {new_ctx['value']}")
        return new_ctx


##############################################################################
# Demo
##############################################################################

if __name__ == "__main__":
    # 1. Create some parameterized Node objects
    nodeA = AddOneNode(name="AddThree", params={"amount": 3})
    nodeB = MultiplyNode(name="TimesTen", params={"factor": 10})
    
    # 2. Compose them
    pipeline = nodeA >> nodeB
    """
    pipeline will be a CompositeNode with sub_nodes = [nodeA, nodeB].
    We could also directly do:  CompositeNode(sub_nodes=[nodeA, nodeB])
    """
    
    # 3. Run the pipeline
    initial_context = {"value": 5}
    print("\n--- Running Pipeline ---")
    result = pipeline(initial_context)
    print(f"Final result context: {result}\n")
    
    # 4. Serialize to JSON
    pipeline_dict = pipeline.to_json()
    pipeline_json = json.dumps(pipeline_dict, indent=2)
    print("--- Pipeline Serialized to JSON ---")
    print(pipeline_json)
    
    # 5. Deserialize from JSON
    restored_dict = json.loads(pipeline_json)
    restored_pipeline = Node.from_json(restored_dict)
    
    # 6. Run the restored pipeline again
    print("\n--- Running Restored Pipeline ---")
    new_context = {"value": 100}
    restored_result = restored_pipeline(new_context)
    print(f"Restored pipeline final result: {restored_result}\n")
