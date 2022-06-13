# Metaflow enables implementation of gradual error handling.
# This is convenient as early prototyping may not require error handling.
# Errors may arise in 1) user code or 2) platform failures.
# Gradually apply these tactics as ordered.
# 1. Python built-in handling: try-except block
#    Use for obvious cases that could raise exceptions
#    or calls to external services like databases.
# 2. Use @retry to handle transient platform errors.
#    Also good practice to wrap dynamicly scaling foreach steps.
# 3. Use @timeout to handle zombie processes.
# 4. Use @catch(var='error_var') to prevent complex steps from crashing workflow.
#    Complex steps could be e.g. data processing or model training.

from metaflow import FlowSpec, step, retry, timeout, catch


class CatchDivideByZero(FlowSpec):

    @step
    def start(self):
        self.dividend = 42
        # Introduce a zero divisor
        self.divisors = list(range(5))
        self.next(self.divide, foreach='divisors')

    # If a divide call fails, set divide_failed to True
    @catch(var='divide_failed')
    # If a divide call fails, first retry it twice
    @retry(times=2)
    @step
    def divide(self):
        self.result = self.dividend / self.input
        self.next(self.join)

    @step
    def join(self, inputs):
        # Explicitly select artifacts to persist
        self.dividend = inputs[0].dividend
        # Select values of result that were not caught
        self.results = [inp.result for inp in inputs if not inp.divide_failed]
        print(f"Results are: {self.results}")
        self.next(self.end)

    # This step would run indefinitely, so crash the workflow after 1 second.
    #
    # @timeout(seconds=1)
    # @step
    # def crash(self):
    #     while(self.dividend > 0):
    #         print("Dividend is positive.")
    #     self.next(self.end)

    # Don't retry this step, even if ~ run --with retry
    @retry(times=0)
    @step
    def end(self):
        print("Exception raised but flow completed gracefully.")


if __name__ == '__main__':
    CatchDivideByZero()
