

import I_xl
import I_csv
from I_SQL import SQLconn
import SQLRepository
from FileDictionaries import startRow_dictionary
from FileDictionaries import delimiter_dictionary
from FileDictionaries import T110sheetDictionary
from FileDictionaries import XLsheetDictionary
from FileEnums import fileType
import pandas as pd
import pyodbc
import sqlalchemy
import pyodbc
import urllib.parse

class ETL:

    rootFolder = ''
    val_TYHours = pd.DataFrame
    val_TYHours_1 = pd.DataFrame
    val_LYHours = pd.DataFrame
    val_LYHours_1 = pd.DataFrame
    val_Dollars = pd.DataFrame
    val_Dollars_1 = pd.DataFrame
    val_T110_Hours = pd.DataFrame
    val_T110_Dollars = pd.DataFrame

    class Extract:

        def __init__(self,rootFolder,fileName_TYHours,fileName_TYHours_1,fileName_LYHours,fileName_LYHours_1,
                     fileName_Dollars,fileName_Dollars_1,fileName_T110):

            ETL.rootFolder = rootFolder

            FileReader = I_csv.read(self.addSlash(rootFolder) + fileName_TYHours,
                                       delimiter_dictionary[fileType.BWHours_Period],
                                       headerRow=startRow_dictionary[fileType.BWHours_Period])
            ETL.val_TYHours = FileReader.values
            print('TY Hours 0 Read')
            FileReader = I_csv.read(self.addSlash(rootFolder) + fileName_TYHours_1,
                                       delimiter_dictionary[fileType.BWHours_Period],
                                       headerRow=startRow_dictionary[fileType.BWHours_Period])
            ETL.val_TYHours_1 = FileReader.values
            print('TY Hours 1 Read')
            FileReader = I_csv.read(self.addSlash(rootFolder) + fileName_LYHours,
                                       delimiter_dictionary[fileType.BWHours_Weekly],
                                       headerRow=startRow_dictionary[fileType.BWHours_Weekly])
            ETL.val_LYHours = FileReader.values
            print('LY Hours 0 Read')
            FileReader = I_csv.read(self.addSlash(rootFolder) + fileName_LYHours_1,
                                       delimiter_dictionary[fileType.BWHours_Weekly],
                                       headerRow=startRow_dictionary[fileType.BWHours_Weekly])
            ETL.val_LYHours_1 = FileReader.values
            print('LY Hours 1 Read')
            FileReader = I_csv.read(self.addSlash(rootFolder) + fileName_Dollars,
                                       delimiter_dictionary[fileType.BWDollars],
                                       headerRow=startRow_dictionary[fileType.BWDollars])
            ETL.val_Dollars = FileReader.values
            print('Dollars 0 Read')
            FileReader = I_csv.read(self.addSlash(rootFolder) + fileName_Dollars_1,
                                       delimiter_dictionary[fileType.BWDollars],
                                       headerRow=startRow_dictionary[fileType.BWDollars])
            ETL.val_Dollars_1 = FileReader.values
            print('Dollars 1 Read')
            FileReader = I_xl.read(self.addSlash(ETL.rootFolder) + fileName_T110)
            FileReader.readData(T110sheetDictionary['Hours'])
            ETL.val_T110_Hours = FileReader.values
            print('T110 Hours Read')
            FileReader.readData(T110sheetDictionary['Dollars'])
            ETL.val_T110_Dollars = FileReader.values
            print('T110 Dollars Read')

        def ExtractHier(self,fileName_Hier):
            FileReader = I_xl.read(self.addSlash(ETL.rootFolder) + fileName_Hier)
            FileReader.readData(XLsheetDictionary[fileType.Hierarchy])
            return FileReader.values

        def ExtractPlant(self, fileName_Plant):
            FileReader = I_csv.read(self.addSlash(ETL.rootFolder) + fileName_Plant,' ',None)
            return FileReader.values

        # def ExtractTimeType(self, fileName_Hier):

        def addSlash(self, locationString):
            if locationString[-1:] == "\\":
                return locationString
            else:
                return locationString + "\\"


    class Transform:

        def __init__(self):
            print('Begin Transformation')
            self.Mod_T110_Hours()
            print('T110 Hours Transformed')
            self.Mod_T110_Dollars()
            print('T110 Dollars Transformed')
            self.Mod_BWHours_Period()
            self.Mod_BWHours_Weekly()
            print('BW Hours Transformed')
            self.Mod_Dollars()
            print('Dollars Transformed')
            print('Transformations Complete')

        def Mod_BWHours_Period(self):
            ETL.val_TYHours['Actual time'] = (ETL.val_TYHours['Actual time'].replace('[)]', '', regex=True).replace(
                '[(]', '-', regex=True).astype(float))
            ETL.val_TYHours_1['Actual time'] = (ETL.val_TYHours_1['Actual time'].replace('[)]', '', regex=True).replace(
                '[(]', '-', regex=True).astype(float))

        def Mod_BWHours_Weekly(self):
            ETL.val_LYHours['Actual time'] = (ETL.val_LYHours['Actual time'].replace('[)]', '', regex=True).replace(
                '[(]', '-', regex=True).astype(float))
            ETL.val_LYHours_1['Actual time'] = (ETL.val_LYHours_1['Actual time'].replace('[)]', '', regex=True).replace(
                '[(]', '-', regex=True).astype(float))

        def Mod_Dollars(self):
            ETL.val_Dollars['YTD Actual'] = (ETL.val_Dollars['YTD Actual'].replace('[)]', '', regex=True).replace(
                '[(]', '-', regex=True).replace('[,]', '', regex=True).astype(float))
            ETL.val_Dollars['YTD Plan'] = (ETL.val_Dollars['YTD Plan'].replace('[)]', '', regex=True).replace(
                '[(]', '-', regex=True).replace('[,]', '', regex=True).astype(float))
            ETL.val_Dollars['YTD PY Actual'] = (ETL.val_Dollars['YTD PY Actual'].replace('[)]', '', regex=True).replace(
                '[(]', '-', regex=True).replace('[,]', '', regex=True).astype(float))
            ETL.val_Dollars['Activity Type'] = (ETL.val_Dollars['Activity Type'].replace('[#]', '0', regex=True)
                                                .astype(int))
            ETL.val_Dollars_1['YTD Actual'] = (ETL.val_Dollars_1['YTD Actual'].replace('[)]', '', regex=True).replace(
                '[(]', '-', regex=True).replace('[,]', '', regex=True).astype(float))
            ETL.val_Dollars_1['YTD Plan'] = (ETL.val_Dollars_1['YTD Plan'].replace('[)]', '', regex=True).replace(
                '[(]', '-', regex=True).replace('[,]', '', regex=True).astype(float))
            ETL.val_Dollars_1['YTD PY Actual'] = (ETL.val_Dollars_1['YTD PY Actual'].replace('[)]', '', regex=True).replace(
                '[(]', '-', regex=True).replace('[,]', '', regex=True).astype(float))
            ETL.val_Dollars_1['Activity Type'] = (ETL.val_Dollars_1['Activity Type'].replace('[#]', '0', regex=True)
                                                .astype(int))

        def Mod_T110_Dollars(self):
            ETL.val_T110_Dollars.columns = ETL.val_T110_Dollars.ix[1, :]
            ETL.val_T110_Dollars = ETL.val_T110_Dollars.ix[2:, :]
            ETL.val_T110_Dollars.drop(columns=['Action', 'Fperiod', 'PTD', 'YTD', 'FY'], axis=1, inplace=True)
            ETL.val_T110_Dollars['PlantINT'] = 0
            for index, row in ETL.val_T110_Dollars.iterrows():
                if str(row['Plant']).lower() == 'plant':
                    ETL.val_T110_Dollars.set_value(index,'PlantINT', 1)


        def Mod_T110_Hours(self):
            ETL.val_T110_Hours.columns = ETL.val_T110_Hours.ix[1,:]
            ETL.val_T110_Hours = ETL.val_T110_Hours.ix[2:, :]
            ETL.val_T110_Hours['PlantINT'] = 0
            for index, row in ETL.val_T110_Hours.iterrows():
                if str(row['Plant']).lower() == 'plant':
                    ETL.val_T110_Hours.set_value(index, 'PlantINT', 1)
            ETL.val_T110_Hours['P1'] = ETL.val_T110_Hours.fillna(0)['Wk0'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk1'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk2'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk3'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk4']
            ETL.val_T110_Hours['P2'] = ETL.val_T110_Hours.fillna(0)['Wk5'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk6'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk7'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk8']
            ETL.val_T110_Hours['P3'] = ETL.val_T110_Hours.fillna(0)['Wk9'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk10'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk11'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk12'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk13']
            ETL.val_T110_Hours['P4'] = ETL.val_T110_Hours.fillna(0)['Wk14'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk15'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk16'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk17']
            ETL.val_T110_Hours['P5'] = ETL.val_T110_Hours.fillna(0)['Wk18'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk19'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk20'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk21']
            ETL.val_T110_Hours['P6'] = ETL.val_T110_Hours.fillna(0)['Wk22'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk23'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk24'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk25'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk26']
            ETL.val_T110_Hours['P7'] = ETL.val_T110_Hours.fillna(0)['Wk27'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk28'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk29'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk30']
            ETL.val_T110_Hours['P8'] = ETL.val_T110_Hours.fillna(0)['Wk31'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk32'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk33'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk34']
            ETL.val_T110_Hours['P9'] = ETL.val_T110_Hours.fillna(0)['Wk35'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk36'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk37'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk38'] + \
                                       ETL.val_T110_Hours.fillna(0)['Wk39']
            ETL.val_T110_Hours['P10'] = ETL.val_T110_Hours.fillna(0)['Wk40'] + \
                                        ETL.val_T110_Hours.fillna(0)['Wk41'] + \
                                        ETL.val_T110_Hours.fillna(0)['Wk42'] + \
                                        ETL.val_T110_Hours.fillna(0)['Wk43']
            ETL.val_T110_Hours['P11'] = ETL.val_T110_Hours.fillna(0)['Wk44'] + \
                                        ETL.val_T110_Hours.fillna(0)['Wk45'] + \
                                        ETL.val_T110_Hours.fillna(0)['Wk46'] + \
                                        ETL.val_T110_Hours.fillna(0)['Wk47']
            ETL.val_T110_Hours['P12'] = ETL.val_T110_Hours.fillna(0)['Wk48'] + \
                                        ETL.val_T110_Hours.fillna(0)['Wk49'] + \
                                        ETL.val_T110_Hours.fillna(0)['Wk50'] + \
                                        ETL.val_T110_Hours.fillna(0)['Wk51'] + \
                                        ETL.val_T110_Hours.fillna(0)['Wk52'] + \
                                        ETL.val_T110_Hours.fillna(0)['Wk53']
            ETL.val_T110_Hours.drop(columns=['Action','Total'],axis=1,inplace=True)
            ETL.val_T110_Hours.drop(columns=['Wk0','Wk1','Wk2','Wk3','Wk4','Wk5','Wk6','Wk7','Wk8','Wk9','Wk10','Wk11',
                                    'Wk12','Wk13','Wk14','Wk15','Wk16','Wk17','Wk18','Wk19','Wk20','Wk21','Wk22','Wk23',
                                    'Wk24','Wk25','Wk26','Wk27','Wk28','Wk29','Wk30','Wk31','Wk32','Wk33','Wk34',
                                    'Wk35','Wk36','Wk37','Wk38','Wk39','Wk40','Wk41','Wk42','Wk43','Wk44','Wk45',
                                    'Wk46','Wk47','Wk48','Wk49','Wk50','Wk51','Wk52','Wk53'],axis=1,inplace=True)

class Load:

    sqlalchemy_engine = sqlalchemy.engine
    connection = SQLconn
    connString = ''
    reportID = 0

    def __init__(self,connString):
        self.connString = connString
        params = urllib.parse.quote_plus(connString)
        self.sqlalchemy_engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

    def getReportID(self, ReportYear, ReportPeriod):
        proxy = self.execSP_param_output('EXEC [4-DLM_Input_2].[dbo].[CreateReportID] ?, ?', [ReportYear, ReportPeriod])
        self.reportID = proxy[0][0]

    def execSP_param_output(self,sql_str,parameters):
        connection = pyodbc.connect(self.connString)
        cursor = connection.cursor()
        cursor.execute(sql_str, parameters)
        proxy = cursor.fetchall()
        cursor.commit()
        return proxy

    def insertIntoStaging(self,values):
        values.to_sql(name='STAGING', con=self.sqlalchemy_engine, if_exists='replace', index=False)

    def execSP_param(self, sql_str, parameters):
        connection = pyodbc.connect(self.connString)
        cursor = connection.cursor()
        cursor.execute(sql_str, parameters)
        cursor.commit()
