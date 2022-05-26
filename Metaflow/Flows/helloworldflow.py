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
