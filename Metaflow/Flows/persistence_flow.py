# Metaflow persists state after each step.
# Ordinary variables are not persisted outside step scope.
# Instance variables are persisted across steps.
# A step's processes are run as tasks, each having its own process ID that can be monitored in 'top'
# A pathspec is a unique identifier which includes flow name, run ID, step name, and task ID.
# Pathspecs may be used for debugging and monitoring flow runs.
#
# CLI commands
# Copy a pathspec e.g. 1653984756654373/end/3
# Reproduce a task:
# ~ python persistenceflow.py logs 1653984756654373/end/3
# Observe instance variables:
# ~ python persistenceflow.py dump 1653984756654373/end/3

from metaflow import FlowSpec, step


class PersistenceFlow(FlowSpec):

    @step
    def start(self):
        self.persisted = "kept"
        temporary = "discarded"
        print(f"Instance variable is {self.persisted}.")
        print(f"Ordinary variable is {temporary} after this step.")
        self.next(self.update_variables)

    @step
    def update_variables(self):
        self.persisted += " twice"
        print(f"Instance variable is {self.persisted}.")

        if 'temporary' in locals():
            print("Ordinary value persisted.")
        else:
            print("Ordinary value did not persist.")

        self.next(self.end)

    @step
    def end(self):
        print("Observe a step's  instance variables with CLI command: 'dump [pathspec]'.")


if __name__ == '__main__':
    PersistenceFlow()
