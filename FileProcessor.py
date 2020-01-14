import I_csv
import I_xl
import csv
from FileEnums import fileType, fileFormat, fileFormat_dictionary, TOPRSskipSheets
from typing import Dict
from FileDictionaries import startRow_dictionary, XLsheetDictionary, T110sheetDictionary
from DataCleaner import dataFilter
import os
import subprocess

class fileProcessor:

    def __init__(self, fileLocation, fileName, fileType, currentYear):

        FileFormat = fileFormat_dictionary[fileType]

        if FileFormat == fileFormat.csv or FileFormat == fileFormat.xl:
            if FileFormat == fileFormat.csv :
                FileReader = I_csv.readCSV(self.addSlash(fileLocation) + fileName, self.csvDialect_dictionary[fileType])
            else: # fileFormat = xl
                FileReader = I_xl.readXL(self.addSlash(fileLocation) + fileName)
                FileReader.readData(XLsheetDictionary[fileType])

            startRow = startRow_dictionary[fileType]
            DataFilter = dataFilter(FileReader.values, startRow)

            if fileType == fileType.SAP9502 or fileType == fileType.DLM_BWHours_Period:
                insertValues = DataFilter.filter95xx()
                DataFilter.deleteValues()
            elif fileType == fileType.SAP9000:
                insertValues = DataFilter.filter9000(currentYear)
                DataFilter.deleteValues()
            elif fileType == fileType.SAP9000_DLM or fileType == fileType.DLM_BWDollars:
                insertValues = DataFilter.filter9000_DLM(currentYear)
                DataFilter.deleteValues()
            elif fileType == fileType.SAPYOYAdj:
                insertValues = DataFilter.filter95xxYOYAdj()
                DataFilter.deleteValues()
            elif fileType == fileType.SAP9502_WKLY or fileType == fileType.DLM_BWHours_Weekly:
                insertValues = DataFilter.filter95xxWKLY()
                #insertValues = DataFilter.filter95xxWKLY_Custom()
                DataFilter.deleteValues()
            elif fileType == fileType.SAP9532_WKLY:
                insertValues = DataFilter.filter95xxWKLY()
                #insertValues = DataFilter.filter95xxWKLY_Custom()
                DataFilter.deleteValues()
            elif fileType == fileType.HHBonus:
                insertValues = DataFilter.filterHHBonus()
                DataFilter.deleteValues()
            elif fileType == fileType.SAPHierarchy:
                insertValues = DataFilter.filterSAPHierarchy()
                DataFilter.deleteValues()
            elif fileType == fileType.YOYAdj:
                insertValues = DataFilter.filterYOYAdj()
                DataFilter.deleteValues()
            elif fileType == fileType.FieldRollup:
                insertValues = DataFilter.filterFieldRollup()
                DataFilter.deleteValues()
            elif fileType == fileType.TOPRSPlan:
                insertValues = DataFilter.filterTOPRSPlan()
                DataFilter.deleteValues()
            elif fileType == fileType.RestructureActuals:
                insertValues = DataFilter.filterRestructure(True)
                DataFilter.deleteValues()
            elif fileType == fileType.RestructureTarget:
                insertValues = DataFilter.filterRestructure(False)
                DataFilter.deleteValues()
            elif fileType == fileType.T313:
                insertValues = DataFilter.filterT313()
                DataFilter.deleteValues()
            elif fileType == fileType.WC2CC:
                insertValues = DataFilter.filterWC2CC()
                DataFilter.deleteValues()
            elif fileType == fileType.LastPlanA:
                insertValues = DataFilter.filterLastPlanA()
                DataFilter.deleteValues()
            elif fileType == fileType.Weekly1508:
                insertValues = DataFilter.filter1508()
                DataFilter.deleteValues()
            elif fileType == fileType.T008:
                insertValues = DataFilter.filterT008()
                DataFilter.deleteValues()
            elif fileType == fileType.TOPRSRollup:
                insertValues = DataFilter.filterTOPRSRollup()
                DataFilter.deleteValues()
            elif fileType == fileType.DLM_Hierarchy_Eng or fileType == fileType.DLM_Hierarchy_Fr:
                insertValues = DataFilter.filterHier()
                DataFilter.deleteValues()
            else:  # fileType == SAP9532
                insertValues = DataFilter.filter95xx()
                DataFilter.deleteValues()

            CSVOutput(insertValues, fileType, '', '', '', '')
            subprocess.call(r'C:\Users\gouldd2\FileSystem\CSharpie\bin\Release\netcoreapp2.1\win10-x64\CSharpie.exe')

        elif FileFormat == fileFormat.TOPRS:
            fileLocation = self.addSlash(fileLocation)
            fileList = os.listdir(fileLocation)
            for fileName in fileList:
                FileReader = I_xl.readXL(fileLocation + fileName)
                for sheetName in FileReader.sheets:
                    if sheetName not in TOPRSskipSheets:
                        if 'Total' in sheetName:
                            isTotal = 1
                        else:
                            isTotal = 0
                        FileReader.readData(sheetName)
                        reportYear = int(float(FileReader.values[2][1]))
                        reportWeek = int(float(FileReader.values[3][1]))
                        reportName = str(FileReader.values[5][1])
                        if "'" in reportName:
                            reportName = reportName.replace("'", "")
                        startRow = startRow_dictionary[fileType]
                        DataFilter = dataFilter(FileReader.values, startRow)
                        insertValues = DataFilter.filterTOPRS()
                        CSVOutput(insertValues, fileType, reportName, reportYear, reportWeek, isTotal)
                        DataFilter.deleteValues()
            subprocess.call(
                r'C:\Users\gouldd2\FileSystem\CSharpie\bin\Release\netcoreapp2.1\win10-x64\CSharpie.exe')
        else:   # fileFormat == T110
            FileReader = I_xl.readXL(self.addSlash(fileLocation) + fileName)
            FileReader.readData(T110sheetDictionary['Hours'])
            HoursData = FileReader.values
            FileReader.readData(T110sheetDictionary['Dollars'])
            DollarsData = FileReader.values

            startRow = startRow_dictionary[fileType]
            DataFilter = dataFilter(FileReader.values, startRow)
            insertValues = DataFilter.filterT110(HoursData,DollarsData,startRow,startRow,currentYear)

            CSVOutput(insertValues, fileType, '', '', '', '')
            # subprocess.call(r'C:\Users\gouldd2\FileSystem\CSharpie\bin\Release\netcoreapp2.1\win10-x64\CSharpie.exe')


    def addSlash(self, locationString):
        if locationString[-1:] == "\\":
            return locationString
        else:
            return locationString + "\\"

    csvDialect_dictionary: Dict[fileType, str] = { fileType.SAP9502 : 'SAPoutput',
                                                 fileType.SAP9532: 'SAPoutput',
                                                 fileType.SAP9000: 'SAPoutput',
                                                 fileType.SAP9502_WKLY: 'SAPoutput',
                                                 fileType.SAP9000_DLM: 'SAPoutput',
                                                 fileType.SAP9532_WKLY: 'SAPoutput',
                                                 fileType.HHBonus: 'HHBonus',
                                                 fileType.DLM_BWHours_Period: 'SAPoutput',
                                                 fileType.DLM_BWHours_Weekly: 'SAPoutput',
                                                 fileType.DLM_BWDollars: 'SAPoutput'}

class CSVOutput:

    def __init__(self, values, fileType, reportName, reportYear, reportWeek, isTotal):
        if fileType == fileType.TOPRS:
            existingFiles = os.listdir('Output')
            maxNumber = 1
            if len(existingFiles) > 1:
                for fileName in existingFiles:
                    if fileName[-3:] == 'csv':
                        teststring = fileName[-6:][:2]
                        currentNumber = int(fileName[-6:][:2])
                        if currentNumber > maxNumber:
                            maxNumber = currentNumber
                maxNumber = maxNumber + 1
            if maxNumber < 10:
                maxNumber_str = '0' + str(maxNumber)
            else:
                maxNumber_str = str(maxNumber)
            outputFileName = 'Output_' + maxNumber_str + '.csv'
            with open('Output/' + outputFileName, 'w',newline='') as writeFile:
                writer = csv.writer(writeFile,  delimiter='|')
                header = ['#HEADER',str(fileType),str(reportName),str(reportYear),str(reportWeek),str(isTotal)]
                writer.writerow(header)
                writer.writerows(values)
        else:
            with open('Output/Output.csv', 'w',newline='') as writeFile:
                writer = csv.writer(writeFile,  delimiter='|')
                header = ['#HEADER',str(fileType)]
                writer.writerow(header)
                writer.writerows(values)
