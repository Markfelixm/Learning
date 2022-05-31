# Metaflow rules:
# 1. DAG (Directed acyclic graphs) may be represented as flows derived from FlowSpec class.
# 2. A step in a flow implemented as method and denoted by @step decorator, must end in self.next() 
# 3. First step must be start, last step must be end (end doesn't require self.next()
# 4. Must link steps with self.next(next_step_name)
# 5. One flow per python module. Instantiate flow class with if __name__ == '__main__'


# CLI commands:
# Validate without execution
# ~ python helloworldflow.py 
# Textual flow representation
# ~ python helloworldflow.py show
# Execute flow
# ~ python helloworldflow.py run


from metaflow import FlowSpec, step

class HelloWorldFlow(FlowSpec):

  @step
  def start(self):
    """starting step"""
    print("First step.")
    self.next(self.hello)

  @step
  def hello(self):
    """say hello"""
    print("Hello, World!")
    self.next(self.end)

  @step
  def end(self):
    """ending step"""
    print("Last step.")
    

if __name__ == '__main__':
  HelloWorldFlow()
