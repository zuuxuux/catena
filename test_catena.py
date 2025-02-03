# test_catena.py

import json
import pytest
from dataclasses import asdict, dataclass
from unittest.mock import patch

# Assuming the following classes are defined in a module named 'catena':
# - DataClassBase
# - PersonInput
# - GreetingOutput
# - FavoriteColorOutput
# - Node
# - CompositeNode
# - GreetNode (PersonInput -> GreetingOutput)
# - ColorNode (GreetingOutput -> FavoriteColorOutput)

# If they're in a file called catena.py, you'd do:
from catena import (
    DataClassBase, PersonInput, GreetingOutput, FavoriteColorOutput, 
    GreetNode, ColorNode, CompositeNode, Node
)

# ---------------------------------------------------------------------------
# 1. Dataclass Schema Tests
# ---------------------------------------------------------------------------

def test_person_input_instantiation():
    """Check that PersonInput can be instantiated and used."""
    person = PersonInput(name="Alice", age=30)
    assert person.name == "Alice"
    assert person.age == 30

def test_person_input_asdict():
    """Verify dataclass -> dict conversion via asdict()."""
    person = PersonInput(name="Bob", age=25)
    d = asdict(person)
    assert d == {"name": "Bob", "age": 25}

def test_greeting_output_instantiation():
    """Check that GreetingOutput can be instantiated."""
    greeting = GreetingOutput(greeting="Hi there!")
    assert greeting.greeting == "Hi there!"

# ---------------------------------------------------------------------------
# 2. Single Node Tests
# ---------------------------------------------------------------------------

def test_greet_node_happy_path():
    """Ensure GreetNode transforms context correctly with valid input."""
    node = GreetNode("Hello {name}, you are {age} years old.")
    context = {"name": "Alice", "age": 30, "extra": 999}

    out = node(context)
    # It should add or overwrite the 'greeting' field
    assert out["name"] == "Alice"
    assert out["age"] == 30
    assert out["extra"] == 999  # 'extra' remains by default
    assert out["greeting"] == "Hello Alice, you are 30 years old."

def test_greet_node_missing_field():
    """Check that GreetNode raises an error if 'age' is missing."""
    node = GreetNode("Format {name}, {age}")
    context = {"name": "Alice"}  # no "age" key

    with pytest.raises(ValueError, match="missing required field 'age'"):
        node(context)

def test_color_node_happy_path():
    """Ensure ColorNode uses 'greeting' and outputs 'favorite_color'."""
    node = ColorNode(color="red")
    context = {"greeting": "Hello world!"}

    out = node(context)
    assert out["greeting"] == "Hello world!"
    assert out["favorite_color"] == "red"

def test_color_node_missing_field():
    """Check that ColorNode raises an error if 'greeting' is missing."""
    node = ColorNode()
    context = {}  # no "greeting" key

    with pytest.raises(ValueError, match="Missing required field 'greeting'"):
        node(context)

# ---------------------------------------------------------------------------
# 3. CompositeNode (Composition) Tests
# ---------------------------------------------------------------------------

def test_composite_node_basic():
    """Compose GreetNode and ColorNode, confirm final context has both outputs."""
    greet = GreetNode("Hi {name} (age={age})")
    color = ColorNode(color="green")
    pipeline = greet >> color  # same as CompositeNode([greet, color])

    ctx = {"name": "Bob", "age": 40}
    final = pipeline(ctx)

    assert final["name"] == "Bob"
    assert final["age"] == 40
    assert final["greeting"] == "Hi Bob (age=40)"
    assert final["favorite_color"] == "green"

def test_composite_node_three_nodes():
    """Chain three nodes (two GreetNodes + one ColorNode) to check accumulation."""
    greet1 = GreetNode("Greetings, {name}!")
    greet2 = GreetNode("Again, {name}, age is {age}.")
    color = ColorNode(color="blue")

    pipeline = greet1 >> greet2 >> color
    ctx = {"name": "Charlie", "age": 22}

    out = pipeline(ctx)
    # The final context should have 'greeting' from the last node that set it,
    # but actually GreetNode always sets 'greeting' each time. 
    # The final color node sets favorite_color.
    assert "favorite_color" in out
    assert out["favorite_color"] == "blue"
    assert out["greeting"] == "Again, Charlie, age is 22."

# ---------------------------------------------------------------------------
# 4. Serialization (Round-Trip) Tests
# ---------------------------------------------------------------------------

def test_node_serialization_round_trip():
    """Single node: GreetNode -> JSON -> from_json -> same output."""
    original_node = GreetNode("Hello {name}, age {age}")
    
    node_dict = original_node.to_json()
    # node_dict => { "type": "...GreetNode", "config": { "greeting_format": ... } }
    # check minimal structure
    assert "type" in node_dict
    assert "config" in node_dict

    # Round-trip
    restored = GreetNode.from_json(node_dict)
    ctx = {"name": "Alice", "age": 30}
    
    out_orig = original_node(ctx)
    out_restored = restored(ctx)
    assert out_orig == out_restored

def test_composite_serialization():
    """Composite node with two sub-nodes: GreetNode >> ColorNode -> JSON -> from_json."""
    greet = GreetNode("Yo {name}, age {age}")
    color = ColorNode(color="purple")

    pipeline = greet >> color
    pipeline_data = pipeline.to_json()

    # pipeline_data => { "type": "catena.CompositeNode", "config": { "sub_nodes": [...]} }
    restored = CompositeNode.from_json(pipeline_data)

    ctx = {"name": "Zoe", "age": 100}
    out_orig = pipeline(ctx)
    out_restored = restored(ctx)
    assert out_orig == out_restored

# ---------------------------------------------------------------------------
# 5. Negative / Error Handling Tests
# ---------------------------------------------------------------------------

def test_composite_no_subnodes():
    """Check that CompositeNode raises an error if constructed with empty list."""
    with pytest.raises(ValueError, match="requires at least one sub-node"):
        CompositeNode([])

# ---------------------------------------------------------------------------
# 6. Mocking a Tool or User Input (Optional)
# ---------------------------------------------------------------------------

@pytest.mark.skip(reason="Example for demonstration of mocking user input.")
def test_ask_user_node():
    from catena import AskUserNode  # hypothetical node
    node = AskUserNode(prompt="What's your name?")
    context = {"prompt": "What's your name?"}

    # Mock input to simulate user typing "TestUser"
    with patch("builtins.input", return_value="TestUser"):
        out = node(context)
        assert out["answer"] == "TestUser"

# ---------------------------------------------------------------------------
# 7. Integration Test (Optional)
# ---------------------------------------------------------------------------

def test_integration_pipeline():
    """
    End-to-end test with a pipeline of GreetNode, ColorNode, maybe repeated.
    Ensures everything works in real usage.
    """
    greet1 = GreetNode("Greetings, {name}.")
    greet2 = GreetNode("Double-checking age: {age}.")
    color = ColorNode(color="orange")
    pipeline = greet1 >> greet2 >> color
    
    context = {"name": "Dana", "age": 45, "debug": True}
    final_ctx = pipeline(context)
    
    assert "greeting" in final_ctx
    assert "favorite_color" in final_ctx
    assert final_ctx["favorite_color"] == "orange"

    # Optionally, serialize + deserialize, then re-check
    data = pipeline.to_json()
    re_pipeline = Node.from_json(data)
    final2 = re_pipeline(context)
    assert final2 == final_ctx

import pytest
from typing import Dict, Any

# Import everything from the module that implements the code above:
# from catena_typed import (
#     Node, CompositeNode, GreetNode, ColorNode,
#     PersonInput, GreetingOutput, FavoriteColorOutput
# )

def test_valid_composition():
    """
    GreetNode: PersonInput -> GreetingOutput
    ColorNode: GreetingOutput -> FavoriteColorOutput
    
    We expect to be able to compose them into a pipeline:
       PersonInput -> GreetingOutput -> FavoriteColorOutput
    since 'GreetingOutput' is the out_schema of the first node
    and the in_schema of the second node.
    """
    greet = GreetNode("Hi {name}, age={age}")
    color = ColorNode(color="green")

    # This should succeed with no TypeError
    pipeline = greet >> color

    # Now test runtime
    context = {"name": "Alice", "age": 30}
    out = pipeline(context)
    assert out["greeting"] == "Hi Alice, age=30"
    assert out["favorite_color"] == "green"


def test_invalid_composition_missing_fields():
    """
    If the second node requires a field that is never produced by the first,
    composition should fail with a TypeError at build time.
    
    For instance, if we try to chain:
      - ColorNode expects GreetingOutput
      - But the first node doesn't produce 'greeting'.
    We'll define a node that produces something else.
    """
    class WrongOutputNode(Node[PersonInput, FavoriteColorOutput]):
        @property
        def in_schema(self):
            return PersonInput
        
        @property
        def out_schema(self):
            return FavoriteColorOutput
        
        def run(self, inp: PersonInput) -> FavoriteColorOutput:
            # Produces "favorite_color" but not "greeting"
            return FavoriteColorOutput(favorite_color="red")

    nodeA = WrongOutputNode()
    nodeB = ColorNode()  # expects 'greeting'
    
    # Attempt composition
    with pytest.raises(TypeError, match="Cannot compose node"):
        pipeline = nodeA >> nodeB


def test_composite_chain_three_nodes():
    """
    Chain three nodes, ensuring type accumulation works properly.
    - greet: PersonInput -> GreetingOutput
    - color: GreetingOutput -> FavoriteColorOutput
    - someThirdNode: ???

    If the third node wants both greeting & favorite_color, 
    we must ensure the second node's output is included.
    """
    @dataclass
    class ExtendedOutput(DataClassBase):
        greeting: str
        favorite_color: str
        combined_message: str

    class CombineNode(Node[FavoriteColorOutput, ExtendedOutput]):
        """
        CombineNode:
          in_schema = FavoriteColorOutput
          out_schema = ExtendedOutput
          It reuses 'greeting'? Wait, we only get 'favorite_color' from input here,
          so we won't have 'greeting' unless we do accumulation.
          
          But composition-time checks say:
            Accumulated after node 1 & node 2 => {name, age, greeting, favorite_color}
            Now node 3 requires 'favorite_color' only? Or also 'greeting'?

          We'll just demonstrate how to require it, though it's not in our in_schema strictly.
          If we truly want 'greeting' too, we'd define in_schema=some schema that includes both.
        """
        def __init__(self):
            super().__init__()

        @property
        def in_schema(self):
            # This node requires just "favorite_color"
            # But let's pretend it also uses "greeting"
            # so let's define a new schema that has both:
            @dataclass
            class InputNeeded(DataClassBase):
                greeting: str
                favorite_color: str
            return InputNeeded

        @property
        def out_schema(self):
            return ExtendedOutput

        def run(self, inp: Any) -> ExtendedOutput:
            return ExtendedOutput(
                greeting=inp.greeting,
                favorite_color=inp.favorite_color,
                combined_message=f"{inp.greeting} => color is {inp.favorite_color}"
            )

    greet = GreetNode("Hi {name} (age={age})")
    color = ColorNode("red")
    combine = CombineNode()

    # composition
    pipe = greet >> color >> combine
    # This is valid because:
    #   after greet: known fields = {name, age, greeting}
    #   after color: known = {name, age, greeting, favorite_color}
    #   combine requires {greeting, favorite_color}, which is indeed a subset
    #   => success

    ctx = {"name": "Zoe", "age": 99}
    out = pipe(ctx)
    assert out["greeting"] == "Hi Zoe (age=99)"
    assert out["favorite_color"] == "red"
    assert out["combined_message"] == "Hi Zoe (age=99) => color is red"


def test_composite_node_serialization():
    """
    Verify that a composed pipeline can be serialized to JSON and restored,
    preserving the type checks (which are re-run in CompositeNode's constructor).
    """
    greet = GreetNode("Hello {name}, age={age}")
    color = ColorNode("green")
    pipe = greet >> color

    # Serialize
    pipe_data = pipe.to_json()  # => { "type": "...CompositeNode", "config": {...} }
    json_str = json.dumps(pipe_data, indent=2)

    # Deserialize
    loaded_dict = json.loads(json_str)
    restored_pipe = Node.from_json(loaded_dict)

    ctx = {"name": "Bob", "age": 40}
    out1 = pipe(ctx)
    out2 = restored_pipe(ctx)
    assert out1 == out2
    assert out2["favorite_color"] == "green"



# ---------------------------------------------------------------------------
# END
# ---------------------------------------------------------------------------
