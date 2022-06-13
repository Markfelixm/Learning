# Metaflow supports dynamicly scaling branching via foreach concept.
# Ideal for data parallelism, i.e. running same tasks over different components of data.
# Specify a python list item to iterate over.
# Concurrently run steps by including 'foreach' and specifying list in 'self.next()'.
# Access list item with 'self.input'.

from metaflow import FlowSpec, step


class ForeachFlow(FlowSpec):

    @step
    def start(self):
        import random
        self.guesses = [random.randint(-10, 10) for _ in range(20)]
        self.next(self.solve, foreach='guesses')

    @step
    def solve(self):
        self.guess = self.input
        self.result = self.guess ** 2
        self.next(self.select_min)

    @step
    def select_min(self, inputs):
        self.best_guess = min(inputs, key=lambda inp: inp.result).guess
        self.next(self.end)

    @step
    def end(self):
        print(f"Best guess was {self.best_guess}")


if __name__ == '__main__':
    ForeachFlow()
