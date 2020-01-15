

import I_xl
import I_csv
from I_SQL import SQLconn
from FileDictionaries import startRow_dictionary
from FileDictionaries import delimiter_dictionary
from FileDictionaries import T110sheetDictionary
from FileDictionaries import XLsheetDictionary
from FileEnums import fileType
import pandas as pd
import sqlalchemy
import pyodbc
import urllib.parse

class ETL:

    rootFolder = ''
    fileName_TYHours = ''
    fileName_TYHours_1 = ''
    fileName_LYHours = ''
    fileName_LYHours_1 = ''
    fileName_Dollars = ''
    fileName_Dollars_1 = ''
    fileName_T110 = ''
    fileName_Hier = ''
    fileName_Plant = ''

    getHier = False
    getPlant = False

    val_TYHours = pd.DataFrame
    val_TYHours_1 = pd.DataFrame
    val_LYHours = pd.DataFrame
    val_LYHours_1 = pd.DataFrame
    val_Dollars = pd.DataFrame
    val_Dollars_1 = pd.DataFrame
    val_T110_Hours = pd.DataFrame
    val_T110_Dollars = pd.DataFrame
    val_Hier = pd.DataFrame
    val_Plant = pd.DataFrame

    sqlalchemy_engine = sqlalchemy.engine
    connection = SQLconn
    connString = ''
    reportID = 0

    def __init__(self,rootFolder,fileName_TYHours,fileName_TYHours_1,fileName_LYHours,fileName_LYHours_1,
                     fileName_Dollars,fileName_Dollars_1,fileName_T110,fileName_Hier,fileName_Plant,getHier,getPlant):
        self.rootFolder = rootFolder
        self.fileName_TYHours = fileName_TYHours
        self.fileName_TYHours_1 = fileName_TYHours_1
        self.fileName_LYHours = fileName_LYHours
        self.fileName_LYHours_1 = fileName_LYHours_1
        self.fileName_Dollars = fileName_Dollars
        self.fileName_Dollars_1 = fileName_Dollars_1
        self.fileName_T110 = fileName_T110
        self.fileName_Hier = fileName_Hier
        self.fileName_Plant = fileName_Plant
        self.getHier = getHier
        self.getPlant = getPlant

    def extract_all(self):

        self.rootFolder = self.extract_addSlash(self.rootFolder)

        FileReader = I_csv.read(self.rootFolder + self.fileName_TYHours,
                                   delimiter_dictionary[fileType.BWHours_Period],
                                   startRow_dictionary[fileType.BWHours_Period],False)
        self.val_TYHours = FileReader.values
        print('TY Hours 0 Read')

        FileReader = I_csv.read(self.rootFolder + self.fileName_TYHours_1,
                                delimiter_dictionary[fileType.BWHours_Period],
                                startRow_dictionary[fileType.BWHours_Period], False)
        self.val_TYHours_1 = FileReader.values
        print('TY Hours 1 Read')

        FileReader = I_csv.read(self.rootFolder + self.fileName_LYHours,
                                delimiter_dictionary[fileType.BWHours_Weekly],
                                startRow_dictionary[fileType.BWHours_Weekly], False)
        self.val_LYHours = FileReader.values
        print('LY Hours 0 Read')

        FileReader = I_csv.read(self.rootFolder + self.fileName_LYHours_1,
                                delimiter_dictionary[fileType.BWHours_Weekly],
                                startRow_dictionary[fileType.BWHours_Weekly], False)
        self.val_LYHours_1 = FileReader.values
        print('LY Hours 1 Read')

        FileReader = I_csv.read(self.rootFolder + self.fileName_Dollars,
                                delimiter_dictionary[fileType.BWDollars],
                                startRow_dictionary[fileType.BWDollars], False)
        self.val_Dollars = FileReader.values
        print('Dollars 0 Read')

        FileReader = I_csv.read(self.rootFolder + self.fileName_Dollars_1,
                                delimiter_dictionary[fileType.BWDollars],
                                startRow_dictionary[fileType.BWDollars], False)
        self.val_Dollars_1 = FileReader.values
        print('Dollars 1 Read')

        FileReader = I_xl.read(self.rootFolder + self.fileName_T110)
        FileReader.readData(T110sheetDictionary['Hours'])
        self.val_T110_Hours = FileReader.values
        print('T110 Hours Read')

        FileReader.readData(T110sheetDictionary['Dollars'])
        self.val_T110_Dollars = FileReader.values
        print('T110 Dollars Read')

        if self.getHier:
            FileReader = I_xl.read(self.rootFolder + self.fileName_Hier)
            FileReader.readData(XLsheetDictionary['Hier'])
            self.val_Hier = FileReader.values
            print('Hierarchy Read')

        if self.getPlant:
            FileReader = I_csv.read(self.rootFolder + self.fileName_Plant, '', 0, True)
            self.val_Plant = FileReader.values
            print('Plant Read')

    def extract_addSlash(self, locationString):
        if locationString[-1:] == "\\":
            return locationString
        else:
            return locationString + "\\"

    def transform_all(self):
        print('Begin Transformation')
        self.transform_T110_Hours()
        print('T110 Hours Transformed')
        self.transform_T110_Dollars()
        print('T110 Dollars Transformed')
        self.transform_BWHours_Period()
        self.transform_BWHours_Weekly()
        print('BW Hours Transformed')
        self.transform_Dollars()
        print('Dollars Transformed')
        print('Transformations Complete')

    def transform_BWHours_Period(self):
        self.val_TYHours['Actual time'] = (self.val_TYHours['Actual time'].replace('[)]', '', regex=True).replace(
            '[(]', '-', regex=True).replace('[,]', '', regex=True).astype(float))
        self.val_TYHours_1['Actual time'] = (self.val_TYHours_1['Actual time'].replace('[)]', '', regex=True).replace(
            '[(]', '-', regex=True).replace('[,]', '', regex=True).astype(float))
        self.val_TYHours['Debited acty type'] = (self.val_TYHours['Debited acty type'].
                                                replace('[#]', '0', regex=True).astype(int))
        self.val_TYHours_1['Debited acty type'] = (self.val_TYHours_1['Debited acty type'].
                                                replace('[#]', '0', regex=True).astype(int))

    def transform_BWHours_Weekly(self):
        self.val_LYHours['Actual time'] = (self.val_LYHours['Actual time'].replace('[)]', '', regex=True).replace(
            '[(]', '-', regex=True).replace('[,]', '', regex=True).astype(float))
        self.val_LYHours_1['Actual time'] = (self.val_LYHours_1['Actual time'].replace('[)]', '', regex=True).replace(
            '[(]', '-', regex=True).replace('[,]', '', regex=True).astype(float))
        self.val_LYHours['Debited acty type'] = (self.val_LYHours['Debited acty type'].
                                                replace('[#]', '0', regex=True).astype(int))
        self.val_LYHours_1['Debited acty type'] = (self.val_LYHours_1['Debited acty type'].
                                                  replace('[#]', '0', regex=True).astype(int))

    def transform_Dollars(self):
        self.val_Dollars['YTD Actual'] = (self.val_Dollars['YTD Actual'].replace('[)]', '', regex=True).replace(
            '[(]', '-', regex=True).replace('[,]', '', regex=True).astype(float))
        self.val_Dollars['YTD Plan'] = (self.val_Dollars['YTD Plan'].replace('[)]', '', regex=True).replace(
            '[(]', '-', regex=True).replace('[,]', '', regex=True).astype(float))
        self.val_Dollars['YTD PY Actual'] = (self.val_Dollars['YTD PY Actual'].replace('[)]', '', regex=True).replace(
            '[(]', '-', regex=True).replace('[,]', '', regex=True).astype(float))
        self.val_Dollars['Activity Type'] = (self.val_Dollars['Activity Type'].replace('[#]', '0', regex=True)
                                            .astype(int))
        self.val_Dollars_1['YTD Actual'] = (self.val_Dollars_1['YTD Actual'].replace('[)]', '', regex=True).replace(
            '[(]', '-', regex=True).replace('[,]', '', regex=True).astype(float))
        self.val_Dollars_1['YTD Plan'] = (self.val_Dollars_1['YTD Plan'].replace('[)]', '', regex=True).replace(
            '[(]', '-', regex=True).replace('[,]', '', regex=True).astype(float))
        self.val_Dollars_1['YTD PY Actual'] = (self.val_Dollars_1['YTD PY Actual'].replace('[)]', '', regex=True).replace(
            '[(]', '-', regex=True).replace('[,]', '', regex=True).astype(float))
        self.val_Dollars_1['Activity Type'] = (self.val_Dollars_1['Activity Type'].replace('[#]', '0', regex=True)
                                            .astype(int))

    def transform_T110_Dollars(self):
        self.val_T110_Dollars.columns = self.val_T110_Dollars.ix[1, :]
        self.val_T110_Dollars = self.val_T110_Dollars.ix[2:, :]
        self.val_T110_Dollars.drop(columns=['Action', 'Fperiod', 'PTD', 'YTD', 'FY'], axis=1, inplace=True)
        self.val_T110_Dollars['PlantINT'] = 0
        for index, row in self.val_T110_Dollars.iterrows():
            if str(row['Plant']).lower() == 'plant':
                self.val_T110_Dollars.set_value(index,'PlantINT', 1)


    def transform_T110_Hours(self):
        self.val_T110_Hours.columns = self.val_T110_Hours.ix[1,:]
        self.val_T110_Hours = self.val_T110_Hours.ix[2:, :]
        self.val_T110_Hours['PlantINT'] = 0
        for index, row in self.val_T110_Hours.iterrows():
            if str(row['Plant']).lower() == 'plant':
                self.val_T110_Hours.set_value(index, 'PlantINT', 1)
        self.val_T110_Hours['P1'] = self.val_T110_Hours.fillna(0)['Wk0'] + \
                                   self.val_T110_Hours.fillna(0)['Wk1'] + \
                                   self.val_T110_Hours.fillna(0)['Wk2'] + \
                                   self.val_T110_Hours.fillna(0)['Wk3'] + \
                                   self.val_T110_Hours.fillna(0)['Wk4']
        self.val_T110_Hours['P2'] = self.val_T110_Hours.fillna(0)['Wk5'] + \
                                   self.val_T110_Hours.fillna(0)['Wk6'] + \
                                   self.val_T110_Hours.fillna(0)['Wk7'] + \
                                   self.val_T110_Hours.fillna(0)['Wk8']
        self.val_T110_Hours['P3'] = self.val_T110_Hours.fillna(0)['Wk9'] + \
                                   self.val_T110_Hours.fillna(0)['Wk10'] + \
                                   self.val_T110_Hours.fillna(0)['Wk11'] + \
                                   self.val_T110_Hours.fillna(0)['Wk12'] + \
                                   self.val_T110_Hours.fillna(0)['Wk13']
        self.val_T110_Hours['P4'] = self.val_T110_Hours.fillna(0)['Wk14'] + \
                                   self.val_T110_Hours.fillna(0)['Wk15'] + \
                                   self.val_T110_Hours.fillna(0)['Wk16'] + \
                                   self.val_T110_Hours.fillna(0)['Wk17']
        self.val_T110_Hours['P5'] = self.val_T110_Hours.fillna(0)['Wk18'] + \
                                   self.val_T110_Hours.fillna(0)['Wk19'] + \
                                   self.val_T110_Hours.fillna(0)['Wk20'] + \
                                   self.val_T110_Hours.fillna(0)['Wk21']
        self.val_T110_Hours['P6'] = self.val_T110_Hours.fillna(0)['Wk22'] + \
                                   self.val_T110_Hours.fillna(0)['Wk23'] + \
                                   self.val_T110_Hours.fillna(0)['Wk24'] + \
                                   self.val_T110_Hours.fillna(0)['Wk25'] + \
                                   self.val_T110_Hours.fillna(0)['Wk26']
        self.val_T110_Hours['P7'] = self.val_T110_Hours.fillna(0)['Wk27'] + \
                                   self.val_T110_Hours.fillna(0)['Wk28'] + \
                                   self.val_T110_Hours.fillna(0)['Wk29'] + \
                                   self.val_T110_Hours.fillna(0)['Wk30']
        self.val_T110_Hours['P8'] = self.val_T110_Hours.fillna(0)['Wk31'] + \
                                   self.val_T110_Hours.fillna(0)['Wk32'] + \
                                   self.val_T110_Hours.fillna(0)['Wk33'] + \
                                   self.val_T110_Hours.fillna(0)['Wk34']
        self.val_T110_Hours['P9'] = self.val_T110_Hours.fillna(0)['Wk35'] + \
                                   self.val_T110_Hours.fillna(0)['Wk36'] + \
                                   self.val_T110_Hours.fillna(0)['Wk37'] + \
                                   self.val_T110_Hours.fillna(0)['Wk38'] + \
                                   self.val_T110_Hours.fillna(0)['Wk39']
        self.val_T110_Hours['P10'] = self.val_T110_Hours.fillna(0)['Wk40'] + \
                                    self.val_T110_Hours.fillna(0)['Wk41'] + \
                                    self.val_T110_Hours.fillna(0)['Wk42'] + \
                                    self.val_T110_Hours.fillna(0)['Wk43']
        self.val_T110_Hours['P11'] = self.val_T110_Hours.fillna(0)['Wk44'] + \
                                    self.val_T110_Hours.fillna(0)['Wk45'] + \
                                    self.val_T110_Hours.fillna(0)['Wk46'] + \
                                    self.val_T110_Hours.fillna(0)['Wk47']
        self.val_T110_Hours['P12'] = self.val_T110_Hours.fillna(0)['Wk48'] + \
                                    self.val_T110_Hours.fillna(0)['Wk49'] + \
                                    self.val_T110_Hours.fillna(0)['Wk50'] + \
                                    self.val_T110_Hours.fillna(0)['Wk51'] + \
                                    self.val_T110_Hours.fillna(0)['Wk52'] + \
                                    self.val_T110_Hours.fillna(0)['Wk53']
        self.val_T110_Hours.drop(columns=['Action','Total'],axis=1,inplace=True)
        self.val_T110_Hours.drop(columns=['Wk0','Wk1','Wk2','Wk3','Wk4','Wk5','Wk6','Wk7','Wk8','Wk9','Wk10','Wk11',
                                'Wk12','Wk13','Wk14','Wk15','Wk16','Wk17','Wk18','Wk19','Wk20','Wk21','Wk22','Wk23',
                                'Wk24','Wk25','Wk26','Wk27','Wk28','Wk29','Wk30','Wk31','Wk32','Wk33','Wk34',
                                'Wk35','Wk36','Wk37','Wk38','Wk39','Wk40','Wk41','Wk42','Wk43','Wk44','Wk45',
                                'Wk46','Wk47','Wk48','Wk49','Wk50','Wk51','Wk52','Wk53'],axis=1,inplace=True)


    def load_connect(self,connString):
        self.connString = connString
        params = urllib.parse.quote_plus(connString)
        self.sqlalchemy_engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

    def load_getReportID(self, ReportYear, ReportPeriod):
        proxy = self.load_execSP_param_output('EXEC [4-DLM_Input_2].[dbo].[CreateReportID] ?, ?', [ReportYear, ReportPeriod])
        self.reportID = proxy[0][0]

    def load_execSP_param_output(self, sql_str, parameters):
        connection = pyodbc.connect(self.connString)
        cursor = connection.cursor()
        cursor.execute(sql_str, parameters)
        proxy = cursor.fetchall()
        cursor.commit()
        return proxy

    def load_insertIntoStaging(self, values):
        values.to_sql(name='STAGING', con=self.sqlalchemy_engine, if_exists='replace', index=False)

    def load_execSP_param(self, sql_str, parameters):
        connection = pyodbc.connect(self.connString)
        cursor = connection.cursor()
        cursor.execute(sql_str, parameters)
        cursor.commit()
