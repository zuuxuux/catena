import importlib
import json
from typing import (
    TypeVar, Generic, Dict, Any, Type, get_type_hints, Optional, List
)
from dataclasses import dataclass, asdict, fields

##############################################################################
# 1) Dataclass-based "schema" definitions
##############################################################################

@dataclass
class DataClassBase:
    """
    Mypy recognizes inheritors of this as valid dataclasses
    so calls to fields(...) and asdict(...) are allowed.
    """
    pass

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
# 2) Helper for dynamic import
##############################################################################

def dynamic_import(fqcn: str):
    """
    Import a class from a fully qualified class name, e.g. 'my_module.GreetNode'
    """
    module_name, class_name = fqcn.rsplit('.', 1)
    mod = importlib.import_module(module_name)
    return getattr(mod, class_name)


##############################################################################
# 3) Type variables and base Node class
##############################################################################

InSchema = TypeVar("InSchema", bound=DataClassBase)
OutSchema = TypeVar("OutSchema", bound=DataClassBase)

class Node(Generic[InSchema, OutSchema]):
    """
    A typed Node from InSchema -> OutSchema, with composable __call__.
    
    Instead of storing parameters as constructor arguments, we store them
    in a generic dictionary 'config'. For (de)serialization, we rely on:
    
      to_json() -> { "type": <fqcn>, "config": self.to_config() }
      from_json(data) -> dynamic_import(data["type"]).from_config(data["config"])
      
    That way, each node can define how it uses 'config' in from_config() and to_config().
    """
    
    def __init__(self):
        # We won't pass everything to __init__ in the base class.
        # Instead, each node sets self.config, etc. in from_config().
        pass

    def run(self, inp: InSchema) -> OutSchema:
        """
        Subclasses override this with the actual transformation logic:
          InSchema -> OutSchema
        """
        raise NotImplementedError()
    
    def __call__(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        1. Build the InSchema from 'context'
        2. run(...) -> OutSchema
        3. Merge output fields into context
        """
        input_obj = self._build_input(context, self.in_schema)
        output_obj = self.run(input_obj)
        
        out_dict = asdict(output_obj)
        new_context = dict(context)
        new_context.update(out_dict)
        return new_context
    
    def _build_input(self, context: Dict[str, Any], schema_cls: Type[InSchema]) -> InSchema:
        # Minimal check: ensure all fields in schema_cls are present in context
        required_fields = {f.name: f.type for f in fields(schema_cls)}
        init_kwargs = {}
        for field_name, field_type in required_fields.items():
            if field_name not in context:
                raise ValueError(f"Missing required field '{field_name}' in context for node {self}.")
            init_kwargs[field_name] = context[field_name]
        
        return schema_cls(**init_kwargs)
    
    @property
    def in_schema(self) -> Type[InSchema]:
        """
        Subclasses must define or store their input schema type.
        """
        raise NotImplementedError()

    @property
    def out_schema(self) -> Type[OutSchema]:
        """
        Subclasses must define or store their output schema type.
        """
        raise NotImplementedError()

    def to_json(self) -> Dict[str, Any]:
        """
        Return a JSON-serializable dict with "type" and "config".
        """
        return {
            "type": f"{self.__class__.__module__}.{self.__class__.__name__}",
            "config": self.to_config()
        }
    
    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "Node":
        """
        Universal entry point: dynamic-import the class, then call from_config.
        """
        node_type = data["type"]
        config = data["config"]
        NodeClass = dynamic_import(node_type)
        return NodeClass.from_config(config)
    
    def to_config(self) -> Dict[str, Any]:
        """
        Subclasses should override to store their own config in a dict.
        This might include sub-nodes if it's a composite node.
        """
        raise NotImplementedError()
    
    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "Node":
        """
        Subclasses override to reconstruct themselves from 'config'.
        """
        raise NotImplementedError("Subclasses must implement from_config().")

    def __rshift__(self, other: "Node") -> "Node":
        """
        Compose: self >> other -> CompositeNode holding [self, other].
        """
        return CompositeNode([self, other])


##############################################################################
# 4) CompositeNode
##############################################################################

class CompositeNode(Node[Any, Any]):
    """
    A node that runs its sub-nodes in sequence. The 'in_schema' is the first node's,
    the 'out_schema' is the last node's. We skip strict static type checks for the
    intermediate steps in this example.
    """
    
    def __init__(self, nodes: List[Node]):
        super().__init__()
        self.nodes = nodes  # each node can have its own typed in/out
        # We define the in/out schemas as the first node's in_schema, last node's out_schema
        if not nodes:
            raise ValueError("CompositeNode requires at least one sub-node.")

    @property
    def in_schema(self) -> Type[Any]:
        return self.nodes[0].in_schema

    @property
    def out_schema(self) -> Type[Any]:
        return self.nodes[-1].out_schema

    def run(self, inp: Any) -> Any:
        # We actually override __call__ instead for multi-step
        # but this function is never directly used. 
        pass

    def __call__(self, context: Dict[str, Any]) -> Dict[str, Any]:
        current_ctx = context
        for i, node in enumerate(self.nodes, 1):
            print(f"[CompositeNode] Step {i} -> Node {node}")
            current_ctx = node(current_ctx)
        return current_ctx

    def __repr__(self):
        return f"CompositeNode(len={len(self.nodes)})"

    def to_config(self) -> Dict[str, Any]:
        """
        We'll store sub-nodes in config["sub_nodes"], each as a to_json() dict.
        """
        return {
            "sub_nodes": [n.to_json() for n in self.nodes]
        }

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "CompositeNode":
        sub_nodes_data = config["sub_nodes"]
        # Rebuild each sub-node
        sub_nodes = [Node.from_json(d) for d in sub_nodes_data]
        return CompositeNode(sub_nodes)


##############################################################################
# 5) Example typed Nodes
##############################################################################

class GreetNode(Node[PersonInput, GreetingOutput]):
    """
    Transform: PersonInput -> GreetingOutput
    """

    def __init__(self, greeting_format: str = "Hello {name}, age {age}!"):
        super().__init__()
        self.greeting_format = greeting_format

    @property
    def in_schema(self) -> Type[PersonInput]:
        return PersonInput

    @property
    def out_schema(self) -> Type[GreetingOutput]:
        return GreetingOutput

    def run(self, inp: PersonInput) -> GreetingOutput:
        greeting_str = self.greeting_format.format(name=inp.name, age=inp.age)
        return GreetingOutput(greeting=greeting_str)

    def __repr__(self):
        return f"GreetNode(format='{self.greeting_format}')"

    # -- Serialization for GreetNode --

    def to_config(self) -> Dict[str, Any]:
        return {
            "greeting_format": self.greeting_format
        }

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "GreetNode":
        greeting_format = config.get("greeting_format", "Hello {name}, age {age}!")
        return GreetNode(greeting_format)


class ColorNode(Node[GreetingOutput, FavoriteColorOutput]):
    """
    Transform: GreetingOutput -> FavoriteColorOutput
    """

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
        # In reality, you'd do something with 'inp.greeting' or self.color
        # Here we just produce a static color for demonstration.
        return FavoriteColorOutput(favorite_color=self.color)

    def __repr__(self):
        return f"ColorNode(color='{self.color}')"

    # -- Serialization for ColorNode --

    def to_config(self) -> Dict[str, Any]:
        return {
            "color": self.color
        }

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "ColorNode":
        color = config.get("color", "blue")
        return ColorNode(color)


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
