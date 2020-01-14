def CreateReportID(ReportYear, ReportPeriod):
    return \
        "	DECLARE @ReportID int			= (SELECT MAX(ReportID) FROM [4-DLM_Input_2].[dbo].[ReportLog]) + 1, " \
        "           @TimeStamp datetime		= CURRENT_TIMESTAMP, " \
        "           @User varchar(50)		= suser_sname() " \
        "INSERT INTO	[4-DLM_Input_2].[dbo].[ReportLog] ( " \
        "				[ReportID]," \
        "				[TimeStamp]," \
        "				[ReportYear]," \
        "   			[ReportPeriod]," \
        "				[UserName]," \
        "				[Details]) " \
        "VALUES (		@ReportID," \
        "				@TimeStamp," \
        "				" + str(ReportYear) + "," \
        "				" + str(ReportPeriod) + "," \
        "				@User," \
        "				NULL) " \
        "" \
        "EXEC [4-DLM_Input_2].[dbo].[CreateLog]" \
        "	@MessageIN = 'ReportID Created'," \
        "	@LogTypeIN = 'SUCCESS'," \
        "	@DataIDIN = @ReportID " \
        "SELECT ReportID FROM [4-DLM_Input_2].[dbo].[ReportLog] WHERE ReportID = @ReportID "
