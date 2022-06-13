# Metaflow is compute layer agnostic.
# There is some built-in support e.g. for AWS batch.
# Decorators may be applied to steps to easily change the compute layer target of a step.
# Tasks are processed on local hardware by default.
# To process on e.g. AWS Batch, annotate a step with @batch.
# Running on external compute requires setup.
#
# Alternatively, compute layer for all steps  may be selected via command line via:
# ~ python compute_flow.py run --with kubernetes
# or with specific compute resouces (8 cores, 8gb RAM):
# ~ python compute_flow.py run --with batch:memory=8000, cpu=8
#
# Vertical scaling may be implemented by specifying compute resources required.
# Use @resources(memory=, cpu=) decorator to agnostically define compute resources required.

from metaflow import FlowSpec, step, resources, batch


class ComputeFlow(FlowSpec):

    @batch
    @resources(cpu=4)
    @step
    def start(self):
        """
        Run tasks of this step using 4 cpu cores on AWS Batch.
        """
        self.next(self.end)

    @step
    def end(self):
        """
        Run tasks of this step on local hardware.
        """


if __name__ == '__main__':
    ComputeFlow()
