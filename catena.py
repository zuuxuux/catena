import importlib
import json
from dataclasses import dataclass, fields, asdict
from typing import TypeVar, Generic, Dict, Any, Type, List, Optional, Union, get_type_hints

##############################################################################
# Base class for data-driven schemas
##############################################################################

@dataclass
class DataClassBase:
    """Marker base class so Mypy and dataclasses know we have standard fields."""
    pass

# Example schemas (you can define more in your own codebase)
@dataclass
class PersonInput(DataClassBase):
    name: str
    age: int

@dataclass
class GreetingOutput(DataClassBase):
    greeting: str

@dataclass
class FavoriteColorOutput(DataClassBase):
    favorite_color: str


##############################################################################
# Dynamic import helper
##############################################################################

def dynamic_import(fqcn: str):
    """Import a class from fully qualified name 'my_module.MyClass'."""
    module_name, class_name = fqcn.rsplit('.', 1)
    mod = importlib.import_module(module_name)
    return getattr(mod, class_name)


##############################################################################
# Type variables
##############################################################################

InSchema = TypeVar("InSchema", bound=DataClassBase)
OutSchema = TypeVar("OutSchema", bound=DataClassBase)


##############################################################################
# Helper: get_schema_fields
##############################################################################

def get_schema_fields(schema_cls: Type[DataClassBase]) -> Dict[str, Type]:
    """
    Return {field_name: field_type, ...} for a dataclass-based schema.
    We assume fields are JSON-friendly types (str, int, float, etc.).
    """
    type_hints = get_type_hints(schema_cls)
    return {f.name: type_hints[f.name] for f in fields(schema_cls)}


def schema_union(
    known_fields: Dict[str, Type],
    new_schema: Type[DataClassBase]
) -> Dict[str, Type]:
    """
    Merge 'known_fields' with the fields from 'new_schema'. 
    If a field is already known, we keep the existing type (or we could unify).
    If it's new, we add it.
    """
    result = dict(known_fields)
    new_fields = get_schema_fields(new_schema)
    for k, t in new_fields.items():
        # If there's a conflict in types, you might handle it specially.
        # For now, we assume no conflicts or we just overwrite.
        if k not in result:
            result[k] = t
    return result


def schema_is_subset(
    required: Dict[str, Type],
    available: Dict[str, Type]
) -> bool:
    """
    Returns True if all 'required' fields are present in 'available' 
    and match (or are compatible with) the type in 'available'.
    
    For simplicity, we'll do a naive check: the field names must match,
    and the type must be the same. Real code might do more advanced checks.
    """
    for rk, rt in required.items():
        if rk not in available:
            return False
        # Could also do an isinstance or issubclass check if we had complex types
        if available[rk] != rt:
            return False
    return True


##############################################################################
# Base Node class with composition-time type checks
##############################################################################

class Node(Generic[InSchema, OutSchema]):
    """
    A typed Node from InSchema -> OutSchema, with composable __call__.
    We store:
      - in_schema: Type[InSchema]
      - out_schema: Type[OutSchema]

    Composition-time checks ensure that if we do (A >> B),
    B.in_schema must be a subset of (A.in_schema + A.out_schema + prior known).
    """

    def __init__(self):
        pass

    def run(self, inp: InSchema) -> OutSchema:
        raise NotImplementedError()

    @property
    def in_schema(self) -> Type[InSchema]:
        raise NotImplementedError()

    @property
    def out_schema(self) -> Type[OutSchema]:
        raise NotImplementedError()

    def __call__(self, context: Dict[str, Any]) -> Dict[str, Any]:
        # 1) build InSchema from context
        input_obj = self._build_input(context, self.in_schema)
        # 2) run -> OutSchema
        output_obj = self.run(input_obj)
        # 3) merge into context
        out_dict = asdict(output_obj)
        new_ctx = dict(context)
        new_ctx.update(out_dict)
        return new_ctx

    def _build_input(
        self,
        context: Dict[str, Any],
        schema_cls: Type[InSchema]
    ) -> InSchema:
        required_fields = get_schema_fields(schema_cls)
        init_kwargs = {}
        for field_name, field_type in required_fields.items():
            if field_name not in context:
                raise ValueError(
                    f"Node {self} missing required field '{field_name}' in context."
                )
            # Optional type-check could go here
            init_kwargs[field_name] = context[field_name]
        return schema_cls(**init_kwargs)

    def to_json(self) -> Dict[str, Any]:
        """
        Minimal JSON structure:
          {
            "type": "full.path.to.NodeClass",
            "config": ... # node-specific
          }
        """
        return {
            "type": f"{self.__class__.__module__}.{self.__class__.__name__}",
            "config": self.to_config()
        }

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "Node":
        node_type = data["type"]
        config = data["config"]
        NodeClass = dynamic_import(node_type)
        return NodeClass.from_config(config)

    def to_config(self) -> Dict[str, Any]:
        # Subclasses override
        return {}

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "Node":
        raise NotImplementedError("Subclasses must implement from_config")

    def __rshift__(self, other: "Node") -> "CompositeNode":
        """
        Compose self >> other, returning a CompositeNode with type-checking:
          - The new node chain can start with self.in_schema 
          - The accumulated fields after 'self' is the union of self.in_schema + self.out_schema
          - We check that other.in_schema is a subset of that accumulation.
        We'll rely on CompositeNode to do extended chaining checks if there are more than two.
        """
        return CompositeNode([self, other])


##############################################################################
# CompositeNode with multi-step type checking
##############################################################################

class CompositeNode(Node[Any, Any]):
    """
    Runs sub-nodes in sequence. We do an extended type check for the entire chain.
    The final in_schema is sub_nodes[0].in_schema,
    the final out_schema is sub_nodes[-1].out_schema.
    """

    def __init__(self, nodes: List[Node]):
        super().__init__()
        if not nodes:
            raise ValueError("CompositeNode requires at least one sub-node.")
        self.nodes: List[Node] = []
        self._build_composite(nodes)

    def _build_composite(self, nodes: List[Node]) -> None:
        """
        Construct a valid chain from left to right, verifying each node’s input 
        is a subset of the accumulated fields so far.
        """
        # Start with the "accumulated" fields = sub_nodes[0].in_schema
        # plus anything out_schema from the first node
        # Then proceed one node at a time, verifying the next node is compatible
        if not nodes:
            return

        # Add first node without checks
        self.nodes.append(nodes[0])
        # Our current "accumulated" fields (like a type environment)
        accumulated = schema_union(
            get_schema_fields(nodes[0].in_schema),
            nodes[0].out_schema
        )

        for node in nodes[1:]:
            required_in = get_schema_fields(node.in_schema)
            # Check subset
            if not schema_is_subset(required_in, accumulated):
                raise TypeError(
                    f"Cannot compose node {node} because it requires fields {required_in}, "
                    f"but we only have {accumulated} so far."
                )
            # If OK, add node
            self.nodes.append(node)
            # Update accumulated with node's out_schema
            accumulated = schema_union(accumulated, node.out_schema)

    @property
    def in_schema(self) -> Type[Any]:
        return self.nodes[0].in_schema

    @property
    def out_schema(self) -> Type[Any]:
        return self.nodes[-1].out_schema

    def run(self, inp: Any) -> Any:
        # Not used directly. We'll override __call__ below.
        pass

    def __call__(self, context: Dict[str, Any]) -> Dict[str, Any]:
        current_ctx = context
        for i, node in enumerate(self.nodes, start=1):
            print(f"[CompositeNode] Step {i} -> Node {node}")
            current_ctx = node(current_ctx)
        return current_ctx

    def to_config(self) -> Dict[str, Any]:
        return {
            "sub_nodes": [n.to_json() for n in self.nodes]
        }

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "CompositeNode":
        sub_nodes_data = config["sub_nodes"]
        sub_nodes = [Node.from_json(d) for d in sub_nodes_data]
        return CompositeNode(sub_nodes)

    def __repr__(self):
        return f"CompositeNode(len={len(self.nodes)})"


##############################################################################
# Example Subclasses
##############################################################################

class GreetNode(Node[PersonInput, GreetingOutput]):
    def __init__(self, greeting_format: str = "Hello {name}, you are {age}"):
        super().__init__()
        self.greeting_format = greeting_format

    @property
    def in_schema(self) -> Type[PersonInput]:
        return PersonInput

    @property
    def out_schema(self) -> Type[GreetingOutput]:
        return GreetingOutput

    def run(self, inp: PersonInput) -> GreetingOutput:
        return GreetingOutput(
            greeting=self.greeting_format.format(name=inp.name, age=inp.age)
        )

    def __repr__(self):
        return f"GreetNode(format='{self.greeting_format}')"

    def to_config(self) -> Dict[str, Any]:
        return {"greeting_format": self.greeting_format}

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "GreetNode":
        fmt = config.get("greeting_format", "Hello {name}, you are {age}")
        return GreetNode(fmt)


class ColorNode(Node[GreetingOutput, FavoriteColorOutput]):
    def __init__(self, color: str = "blue"):
        super().__init__()
        self.color = color

    @property
    def in_schema(self) -> Type[GreetingOutput]:
        return GreetingOutput

    @property
    def out_schema(self) -> Type[FavoriteColorOutput]:
        return FavoriteColorOutput

    def run(self, inp: GreetingOutput) -> FavoriteColorOutput:
        # Normally you'd do something with inp.greeting
        return FavoriteColorOutput(favorite_color=self.color)

    def __repr__(self):
        return f"ColorNode(color='{self.color}')"

    def to_config(self) -> Dict[str, Any]:
        return {"color": self.color}

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "ColorNode":
        return ColorNode(config.get("color", "blue"))




##############################################################################
# 6) Demo
##############################################################################

if __name__ == "__main__":
    greet_node = GreetNode("Hello {name}, who is {age} years old.")
    color_node = ColorNode(color="green")
    
    # Compose them
    pipeline = greet_node >> color_node
    
    # Run
    ctx = {"name": "Alice", "age": 30}
    final = pipeline(ctx)
    print(f"\nFinal context: {final}")
    
    # Serialize
    pipeline_json = pipeline.to_json()
    json_str = json.dumps(pipeline_json, indent=2)
    print("\n--- Pipeline JSON ---")
    print(json_str)

    # Deserialize
    loaded_data = json.loads(json_str)
    restored_pipeline = Node.from_json(loaded_data)

    # Run again
    ctx2 = {"name": "Bob", "age": 25}
    final2 = restored_pipeline(ctx2)
    print(f"\nFinal context from restored pipeline: {final2}")
