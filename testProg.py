
from ETL import ETL
from SQLDictionaries import spInsert_dictionary
from FileEnums import fileType
import json

### User Inputs
rootFolder = 'M:\\APP ZONE\\Direct Labour Model Sandbox\\p11\\MOPS\Month End Process\\Source'
reportYear = 2019
reportPeriod = 11
getHier = False
getPlant = False

with open('DLM_FilePaths.json', 'r') as read_file:
    paramData = json.load(read_file)

connString = paramData["ConnectionString"]
fileName_TYHours = paramData["fileName_TYHours"]
fileName_TYHours_1 = paramData["fileName_TYHours_1"]
fileName_LYHours = paramData["fileName_LYHours"]
fileName_LYHours_1 = paramData["fileName_LYHours_1"]
fileName_Dollars = paramData["fileName_Dollars"]
fileName_Dollars_1 = paramData["fileName_Dollars_1"]
fileName_T110 = paramData["fileName_T110"]
fileName_Hier = paramData["fileName_Hier"]
fileName_Plant = paramData["fileName_Plant"]

vETL = ETL(rootFolder, fileName_TYHours, fileName_TYHours_1, fileName_LYHours, fileName_LYHours_1,
                        fileName_Dollars, fileName_Dollars_1, fileName_T110,fileName_Hier,fileName_Plant,
                        getHier, getPlant)
vETL.extract_all()
vETL.transform_all()

print('Connecting to Database')
vETL.load_connect(connString)
vETL.load_getReportID(reportYear, reportPeriod)
print('ReportID =', vETL.reportID)

# BW Hours (TY):
vETL.load_insertIntoStaging(vETL.val_TYHours)
vETL.load_execSP_param(spInsert_dictionary[fileType.BWHours_Period], [vETL.reportID])
print('TY (0) Hours Inserted')
vETL.load_insertIntoStaging(vETL.val_TYHours_1)
vETL.load_execSP_param(spInsert_dictionary[fileType.BWHours_Period], [vETL.reportID])
print('TY (1) Hours Inserted')

# BW Hours (LY):
vETL.load_insertIntoStaging(vETL.val_LYHours)
vETL.load_execSP_param(spInsert_dictionary[fileType.BWHours_Weekly], [vETL.reportID])
print('LY (0) Hours Inserted')
vETL.load_insertIntoStaging(vETL.val_LYHours_1)
vETL.load_execSP_param(spInsert_dictionary[fileType.BWHours_Weekly], [vETL.reportID])
print('LY (1) Hours Inserted')

# BW Dollars:
vETL.load_insertIntoStaging(vETL.val_Dollars)
vETL.load_execSP_param(spInsert_dictionary[fileType.BWDollars], [vETL.reportID])
print('Dollars (0) Inserted')
vETL.load_insertIntoStaging(vETL.val_Dollars_1)
vETL.load_execSP_param(spInsert_dictionary[fileType.BWDollars], [vETL.reportID])
print('Dollars (1) Inserted')

# T110 Dollars:
vETL.load_insertIntoStaging(vETL.val_T110_Dollars)
vETL.load_execSP_param(spInsert_dictionary[fileType.T110],[vETL.reportID])
print('T110 Dollars Inserted')

# T110 Hours
vETL.load_insertIntoStaging(vETL.val_T110_Hours)
vETL.load_execSP_param(spInsert_dictionary[fileType.T110], [vETL.reportID])
print('T110 Hours Inserted')

print('complete')
