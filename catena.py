"""
A minimal DSL for chaining computations with a Node class in Python.

Key Features:
- Each Node is a function from context -> context.
- The __call__ method applies the node's logic to the context.
- The __rshift__ method (>>) composes two Nodes in sequence.
"""

class Node:
    """
    Represents a single computation from 'context' to 'context'.
    
    Each Node can:
      - Be called directly with a context (using __call__).
      - Be composed with another Node using the >> operator.
    """
    
    def __init__(self, name=None, transform=None):
        """
        Initialize the Node with an optional name and transformation function.
        
        :param name: A human-readable identifier for the Node (string).
        :param transform: A function that takes a context and returns a new context.
                          If None, the Node returns the context unchanged.
        """
        self.name = name if name is not None else self.__class__.__name__
        # Default transform is an identity function if none is provided.
        self.transform = transform if transform else lambda c: c

    def __call__(self, context):
        """
        Apply the Node's transformation to the given context.
        
        :param context: The input context (e.g., a dict or custom object).
        :return: A new or updated context.
        """
        print(f"[Node {self.name}] Received context: {context}")
        new_context = self.transform(context)
        print(f"[Node {self.name}] Returning context: {new_context}")
        return new_context

    def __rshift__(self, other):
        """
        Compose this Node with another Node using the >> operator.
        
        The resulting Node, when called, will:
          1. Call self with the input context.
          2. Pass self's output to 'other'.
          3. Return the result of calling 'other'.
        
        :param other: Another Node to compose with.
        :return: A new composite Node.
        """
        def composite_transform(ctx):
            intermediate = self(ctx)
            return other(intermediate)
        
        composite_name = f"({self.name} >> {other.name})"
        return Node(name=composite_name, transform=composite_transform)


# ------------------- USAGE EXAMPLE / TEST -------------------
if __name__ == "__main__":
    # Example transforms that modify the "value" key in a dictionary context.
    
    def add_one(context):
        """Increment the 'value' in the context by 1."""
        new_ctx = dict(context)
        new_ctx["value"] = new_ctx.get("value", 0) + 1
        return new_ctx
    
    def double_value(context):
        """Double the 'value' in the context."""
        new_ctx = dict(context)
        new_ctx["value"] = new_ctx.get("value", 1) * 2
        return new_ctx
    
    # Create two simple Nodes
    A = Node(name="AddOne", transform=add_one)
    B = Node(name="DoubleValue", transform=double_value)
    
    # Compose them using >>
    # The pipeline will add 1 to the value, then double it.
    pipeline = A >> B
    
    # Test the pipeline with an initial context
    initial_context = {"value": 1}
    result_context = pipeline(initial_context)
    
    print(f"Final result context: {result_context}")
