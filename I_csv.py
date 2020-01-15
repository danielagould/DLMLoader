import csv
import pandas as pd

class read():

    values = []

    def __init__(self,fileLocation, delimiter, headerRow, isTxt):
        if isTxt:
            self.values = pd.read_csv(fileLocation,header=None)
        else:
            self.values = pd.read_csv(fileLocation,delimiter,header=headerRow)
