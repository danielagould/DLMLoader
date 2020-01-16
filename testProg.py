
from ETL import ETL
from ETL import Parameters

### User Inputs
rootFolder = 'M:\\APP ZONE\\Direct Labour Model Sandbox\\p11\\MOPS\Month End Process\\Source'
reportYear = 2019
reportPeriod = 11
getHier = False
getPlant = False

vParam = Parameters()

vETL = ETL(rootFolder, vParam.fileName_TYHours, vParam.fileName_TYHours_1, vParam.fileName_LYHours,
           vParam.fileName_LYHours_1, vParam.fileName_Dollars, vParam.fileName_Dollars_1, vParam.fileName_T110,
           vParam.fileName_Hier, vParam.fileName_Plant, getHier, getPlant, reportYear, reportPeriod, vParam.connString)
vETL.extract_all()
vETL.transform_all()
vETL.load_all()

print('complete')
