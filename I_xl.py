
import xlrd
import pyxlsb
from enum import Enum
import pandas as pd

class read():

    values = []
    sheets = []
    fileType = ''
    fileLocation = ''
    workbookXLSB = pyxlsb.workbook
    #workbookXLSXM = ''
    pdXL = pd.ExcelFile

    def __init__(self, fileLocation):
        self.fileLocation = fileLocation
        if "xlsb" in fileLocation:
            self.fileType = XLFileType.xlsb
        elif "xlsm" in fileLocation:
            self.fileType = XLFileType.xlsm
        elif "xlsx" in fileLocation:
            self.fileType = XLFileType.xlsx
        elif "xls" in fileLocation:
            self.fileType = XLFileType.xls
        else:
            self.fileType = None

        if self.fileType == XLFileType.xlsb:
            self.workbookXLSB = pyxlsb.open_workbook(self.fileLocation)
            self.sheets = self.workbookXLSB.sheets
        else:
            pdXL = pd.ExcelFile(self.fileLocation)
            self.sheets = pdXL.sheet_names
            # self.workbookXLSXM = xlrd.open_workbook(self.fileLocation)
            # self.sheets = self.workbookXLSXM.sheet_names()

    def readData(self, sheetName):
        self.values = [None]
        sheetIndex = self.getSheetIndex(sheetName)
        if self.fileType == XLFileType.xlsb:
            worksheet = self.workbookXLSB.get_sheet(sheetIndex + 1)
            for r in worksheet.rows():
                rowValues = []
                for i in range(0, len(r)):
                    currentValue = r[i].v
                    rowValues.append(currentValue)
                self.values.append(rowValues)
            self.values = pd.DataFrame(self.values[1:],columns=self.values[0])
        else:
            self.values = pd.read_excel(self.fileLocation, sheetName)
            # worksheet = self.workbookXLSXM.sheet_by_index(sheetIndex)
            # for r in range(0, worksheet.nrows):
            #     rowValues = []
            #     for c in range(0, worksheet.ncols):
            #         rowValues.append(worksheet.cell(r,c).value)
            #     self.values.append(rowValues)

    def getSheetIndex(self, sheetName):
        sheetIndex = 0
        for i in range(0, len(self.sheets)):
            if self.sheets[i] != sheetName:
                sheetIndex = sheetIndex + 1
            else:
                return sheetIndex
        return 0


class XLFileType(Enum):
    xlsx = 1
    xlsm = 2
    xlsb = 3
    xls = 4