# Metaflow steps can run concurrently if they may run independently.
# Concurrent steps run as seperate processes, i.e. in parallel.
# All branches must merge and branch artifacts must be explicitly selected.
# A step splits when multiple arguments are provided to self.next()
# Branches are merged when a step method receives the additional 'inputs' argument.
# Inputs is iterable and indexed.
# With static branches, inputs may be targeted by method name.
#
# Notice that each step in a run has its own process id (pid).
# These processes may be tracked with process monitors like 'top'.

from metaflow import FlowSpec, step


class SplitJoinFlow(FlowSpec):

    @step
    def start(self):
        print("Which way should I go?")
        self.navigation = 'take a step'
        self.next(self.left, self.forward,  self.right)

    @step
    def left(self):
        self.navigation += ' and turn left'
        self.correct = False
        print(f"I {self.navigation}."
              f"This is the {'right' if self.correct else 'wrong'} way.")
        self.next(self.join)

    @ step
    def forward(self):
        self.navigation += ' and take another'
        self.correct = True
        print(f"I {self.navigation}."
              f"This is the {'right' if self.correct else 'wrong'} way.")
        self.next(self.join)

    @step
    def right(self):
        self.navigation += ' and turn right'
        self.correct = False
        print(f"I {self.navigation}."
              f"This is the {'right' if self.correct else 'wrong'} way.")
        self.next(self.join)

    @step
    def join(self, inputs):
        # Explicitly select artifacts to persist
        for branch in inputs:
            if branch.correct:
                self.navigation = branch.navigation
        # Automatically merge any undivergent artifacts that aren't excluded
        self.merge_artifacts(inputs, exclude=['correct'])
        self.next(self.end)

    @step
    def end(self):
        print(f"The right way was to {self.navigation}.")


if __name__ == '__main__':
    SplitJoinFlow()
