# Metaflow has several ways of incorporating external data into a flow.
# With files under a gigabyte, it may be appropriate to implement them as parametersself.
# Using IncludeFile will persist the data file as an artifact.
#
# Provide matplotlib-friendly csv file.
# ~ python includefileflow.py run --csv example.csv

from metaflow import FlowSpec, step, Parameter, IncludeFile
from io import StringIO
import pandas as pd
import matplotlib.pyplot as plt


class IncludeFileFlow(FlowSpec):

    csv_file = IncludeFile('csv',
                           help="Specify CSV file to parse.",
                           is_text=True)

    @step
    def start(self):
        file_object = StringIO(self.csv_file)
        self.data = pd.read_csv(file_object)
        print(f"Data has shape of {self.data.shape}")
        self.next(self.plot_data)

    @step
    def plot_data(self):
        print("Close plot window to continue.")
        ax = self.data.plot()
        plt.show()
        self.next(self.end)

    @step
    def end(self):
        print("Inspect csv_file and data artifacts with 'dump' CLI command.")


if __name__ == '__main__':
    IncludeFileFlow()
