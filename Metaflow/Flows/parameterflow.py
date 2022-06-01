# Metaflow parameters are:
# - special artifacts that may be passed into a run
# - immutable class-level constructs
# - available to all steps
# 
# Metaflow parameters require an artifact and parameter name, which may be the same.
# May be assigned explicit python types and default value.
# If no default value and no value provided, given None.
# Parameters may be set as required and can be passed into the flow via terminal env variables.
#
# Use parameter name to interact from command line.
# e.g. python parameterflow.py run --pi 3.14159

from metaflow import FlowSpec, step, Parameter


class ParameterFlow(FlowSpec):

    artifact_name = Parameter('parameter_name',
                              help="Example parameter",
                              default="example value",
                              type=str)

    pi = Parameter('pi',
                   help='Provide value of pi',
                   required=True,
                   type=float)

    @step
    def start(self):
        print(f"Example parameter value is: {self.artifact_name}")
        self.next(self.end)

    @step
    def end(self):
        print(f"Pi was set to {self.pi}")


if __name__ == '__main__':
    ParameterFlow()
