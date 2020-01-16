from enum import Enum
from typing import Dict
from FileEnums import fileType

spInsert_dictionary: Dict[fileType, str] = {    fileType.T110_Hours: 'EXEC [dbo].[spT110_Hours] ?',
                                                fileType.T110_Dollars: 'EXEC [dbo].[spT110_Dollars] ?',
                                                fileType.BWHours_Period: 'EXEC [dbo].[spBWHours_Period] ?',
                                                fileType.BWHours_Weekly: 'EXEC [dbo].[spBWHours_Weekly] ?',
                                                fileType.BWDollars: 'EXEC [dbo].[spBWDollars] ?',
                                                fileType.Hierarchy: 'EXEC [dbo].[spHier] ?'}