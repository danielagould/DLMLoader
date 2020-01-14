from enum import Enum
from typing import Dict

class fileType(Enum):
    BWHours_Period = 1
    BWHours_Weekly = 2
    BWDollars = 3
    Hierarchy = 4
    T110 = 5

class fileFormat(Enum):
    csv = 1
    xl = 2
    T110 = 3


fileType_dictionary: Dict[str, fileType] = {    'T110': fileType.T110,
                                                'BW Hours - Period': fileType.BWHours_Period,
                                                'BW Hours - Weekly': fileType.BWHours_Weekly,
                                                'BW Dollars': fileType.BWDollars,
                                                'Hierarchy': fileType.Hierarchy}

fileFormat_dictionary: Dict[fileType, fileFormat] = {
                                                fileType.T110 : fileFormat.T110,
                                                fileType.BWDollars : fileFormat.csv,
                                                fileType.BWHours_Period : fileFormat.csv,
                                                fileType.BWHours_Weekly: fileFormat.csv,
                                                fileType.Hierarchy: fileFormat.xl}

fileTypeList = {'T110','BW Hours - Period','BW Hours - Weekly','BW Dollars','Hierarchy'}
