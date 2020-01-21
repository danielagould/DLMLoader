from FileEnums import fileType
from typing import Dict

startRow_dictionary: Dict[fileType, int] = {fileType.BWHours_Period: 4,
                                            fileType.BWHours_Weekly: 4,
                                            fileType.BWDollars: 4,
                                            fileType.T110 : 10,
                                            fileType.Hierarchy: 3}

XLsheetDictionary: Dict[fileType, str] = {fileType.Hierarchy: 'Rollup for R3HIER E',
                                          fileType.T100: 'T100___hours',
                                          fileType.T200: 'Data',
                                          fileType.T400: 'T400___YOY_Hours',
                                          fileType.T500: 'T500___YOY_Dollars'}

T110sheetDictionary: Dict[str, str] = {'Hours' : 'Data_Hrs',
                                       'Dollars' : 'Data_Dollars'}

delimiter_dictionary: Dict[fileType, str] = {fileType.BWHours_Weekly: ';',
                                             fileType.BWHours_Period: ';',
                                             fileType.BWDollars: ';'}