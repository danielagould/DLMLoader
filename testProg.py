
from ETL import ETL
from ETL import Parameters
from ETL import ResultLoader


### User Inputs
rootFolder = 'M:\\Direct Labour Model Sandbox\\src_p12'
reportYear = 2019
reportPeriod = 12
getHier = True
getPlant = False

runDLM = False
runResultsLoader = True

vParam = Parameters()

if runDLM:
    vETL = ETL(rootFolder, vParam.fileName_TYHours, vParam.fileName_TYHours_1, vParam.fileName_LYHours,
               vParam.fileName_LYHours_1, vParam.fileName_Dollars, vParam.fileName_Dollars_1, vParam.fileName_T110,
               vParam.fileName_Hier, vParam.fileName_Plant, getHier, getPlant, reportYear, reportPeriod, vParam.connString)
    vETL.extract_all()
    vETL.transform_all()
    vETL.load_all()

    print('ETL complete. Commencing calculation')
    vETL.load_execSP_param('EXEC [Populate T100]', vETL.reportID)
    print('T100 created')
    vETL.load_execSP_param('EXEC [Populate T400]', vETL.reportID)
    print('T400 created')
    vETL.load_execSP_param('EXEC [Populate T200]', vETL.reportID)
    print('T200 created')
    vETL.load_execSP_param('EXEC [Populate T500]', vETL.reportID)
    print('T500 created')

    print('Calculation complete')

if runResultsLoader:
    vRL = ResultLoader(2019, 12, 'C:\\1.WorkWorkWork\\8. Data',vParam.connString)
    vRL.extract_all()
    vRL.transform_all()
    vRL.load_all()