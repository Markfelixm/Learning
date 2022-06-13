# Metaflow parameters supports complex types given as JSON.
# Import and and assign JSONType as the parameter type.
#
# Pass in complex values as dict to JSON.
# ~ python complexparameterflow.py run --mapping '{"k1": "v1", "k2": "v2"}'
# (Remember to use outer single quotes to escape special shell characters.)
#
# Pass JSON files using shell expression.
# ~ python complexparameterflow.py run --mapping "$(cat example.json)"
# (Remember to use outer double quotes for shell expression.)

from metaflow import FlowSpec, step, Parameter, JSONType


class JSONParameterFlow(FlowSpec):

    mapping = Parameter('mapping',
                        help="Specify a mapping.",
                        default='{"some": "default"}',
                        type=JSONType)

    @step
    def start(self):
        for key, value in self.mapping.items():
            print(f"key: {key}, value: {value}")
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == '__main__':
    JSONParameterFlow()
