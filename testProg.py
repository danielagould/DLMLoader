
from ETL import ETL
from ETL import Load

rootFolder = 'M:\\APP ZONE\\Direct Labour Model Sandbox\\p11\\MOPS\Month End Process\\Source'
fileName_TYHours = 'Z1_ZHOUYAN_DLM_9502_Hours_Driver_TY_TEST.csv'
fileName_TYHours_1 = 'Z1_ZHOUYAN_DLM_9502_Hours_Driver_TY_1_TEST.csv'
fileName_LYHours = 'Z1_ZHOUYAN_DLM_9502_Hours_Driver_LY_TEST.csv'
fileName_LYHours_1 = 'Z1_ZHOUYAN_DLM_9502_Hours_Driver_LY_TEST_1.csv'
fileName_Dollars = 'Z1_ZHOUYAN_DLM_9000_Cost_Driver_TEST.csv'
fileName_Dollars_1 = 'Z1_ZHOUYAN_DLM_9000_Cost_Driver_TEST_1.csv'
fileName_T110 = 'Others\\T110 - YOY Adj.xlsb'

connString = 'Driver={SQL Server Native Client 11.0};Server=CPC-5CG8253WS6\\SQLEXPRESS;Database=4-DLM_Input_2;trusted_connection=yes'

getHier = False
fileName_Hier = 'Others\\R3HIER E.xls'

getPlant = False
fileName_Plant = 'Hours\\Major Plant CC.txt'

vETL = ETL()

vETL.Extract(rootFolder, fileName_TYHours, fileName_TYHours_1, fileName_LYHours, fileName_LYHours_1,
                         fileName_Dollars, fileName_Dollars_1, fileName_T110)
print('Reading Data Complete')

# if getPlant:
#     val_Plant = vETL.ExtractPlant(fileName_Plant)
#
# if getHier:
#     val_Hier = vETL.ExtractPlant(fileName_Hier)

vETL.Transform()

print('Connecting to Database')
vLoad = Load(connString)
vLoad.getReportID(2019,11)
print('ReportID =', vLoad.reportID)

# BW Hours (TY):
vLoad.insertIntoStaging(vETL.val_TYHours)
vLoad.execSP_param('EXEC [4-DLM_Input_2].[dbo].[spBWHours_Period] ?', [vLoad.reportID])
print('TY (0) Hours Inserted')
vLoad.insertIntoStaging(vETL.val_TYHours_1)
vLoad.execSP_param('EXEC [4-DLM_Input_2].[dbo].[spBWHours_Period] ?', [vLoad.reportID])
print('TY (1) Hours Inserted')

# BW Hours (LY):
vLoad.insertIntoStaging(vETL.val_LYHours)
vLoad.execSP_param('EXEC [4-DLM_Input_2].[dbo].[spBWHours_Weekly] ?', [vLoad.reportID])
print('LY (0) Hours Inserted')
vLoad.insertIntoStaging(vETL.val_LYHours_1)
vLoad.execSP_param('EXEC [4-DLM_Input_2].[dbo].[spBWHours_Weekly] ?', [vLoad.reportID])
print('LY (1) Hours Inserted')

# BW Dollars:
vLoad.insertIntoStaging(vETL.val_Dollars)
vLoad.execSP_param('EXEC [4-DLM_Input_2].[dbo].[spBWDollars] ?', [vLoad.reportID])
print('Dollars (0) Inserted')
vLoad.insertIntoStaging(vETL.val_Dollars_1)
vLoad.execSP_param('EXEC [4-DLM_Input_2].[dbo].[spBWDollars] ?', [vLoad.reportID])
print('Dollars (1) Inserted')

# T110 Dollars:
vLoad.insertIntoStaging(vETL.val_T110_Dollars)
vLoad.execSP_param('EXEC [4-DLM_Input_2].[dbo].[spT110_Dollars] ?',[vLoad.reportID])
print('T110 Dollars Inserted')

# T110 Hours
vLoad.insertIntoStaging(vETL.val_T110_Hours)
vLoad.execSP_param('EXEC [4-DLM_Input_2].[dbo].[spT110_Hours] ?', [vLoad.reportID])
print('T110 Hours Inserted')

print('complete')