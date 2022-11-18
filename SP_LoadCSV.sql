USE HumanResources

GO

/****** Object:  StoredProcedure [dbo].[SP_EXTRATO_NEGOCIACAO]    Script Date: 19/10/2022 14:15:57 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER  PROCEDURE [dbo].[SP_LoadCSV]

AS

BEGIN

		-- CREATE TEMP TABLE TO STORE DATA FROM CSV FILE
		DROP TABLE IF EXISTS ##TMP_HR
		
		CREATE TABLE ##TMP_HR (
			EmpID					VARCHAR(1000)
		,	EmployeeName				VARCHAR(1000)
		,	Sex					VARCHAR(1000)
		,	HireDate				VARCHAR(1000)
		,	PositionID				VARCHAR(1000)
		,	PositionDesc				VARCHAR(1000)
		,	DepartmentDesc				VARCHAR(1000)
		,	ShiftDesc				VARCHAR(1000)
		,	ContractTypeDesc			VARCHAR(1000)
		,	MaritalStatusDesc			VARCHAR(1000)
		,	BirthDate				VARCHAR(1000)
		,	CitizenshipDesc				VARCHAR(1000)
		,	RaceDesc				VARCHAR(1000)
		,	Salary					VARCHAR(1000)
		,	TerminationDate				VARCHAR(1000)
		,	SituationID				VARCHAR(1000)
		,	SituationDesc				VARCHAR(1000)
		,	TerminationReason			VARCHAR(1000)

		)
		

		-- INSERTS DATA FROM THE CSV FILE TO TEMP TABLE
		BULK INSERT ##TMP_HR
		FROM 'D:\01. Data\05. Portfolio\01. SQL\01. Advanced ETL on SQL Server\BasesRH\EmployeeDatabase.TXT'
		WITH
		(
		   --CODEPAGE = 'ACP',
		   FIRSTROW = 2, -- as 1st one is header
		   FIELDTERMINATOR = '\t',  --CSV field delimiter
		   ROWTERMINATOR = '\n',
		   ERRORFILE = 'D:\01. Data\05. Portfolio\01. SQL\01. Advanced ETL on SQL Server\BasesRH\bulk_insert_BadData.log'
		)	

END

------------------------------------------------------------------------
--	CREATE DIM_DEPARTMENT
------------------------------------------------------------------------

	IF OBJECT_ID('HumanResources..dim_Department') IS NULL

	BEGIN

		CREATE TABLE dim_Department (
			DepartmentID	SMALLINT	NOT NULL IDENTITY(1,1) 
		,	DepartmentDesc	VARCHAR(50)

			CONSTRAINT PK_Department PRIMARY KEY (DepartmentID)
		)

	END

-- LOADING DATA INTO THE DIM_DEPARTMENT

	MERGE dim_Department TRG
	USING(
			SELECT distinct
				B.DepartmentID
			,	A.DepartmentDesc
			FROM ##TMP_HR A
			
			LEFT 
			JOIN dim_Department B
			ON A.DepartmentDesc = B.DepartmentDesc
		) SRC

	ON SRC.DepartmentDesc = TRG.DepartmentDesc
		 
	WHEN MATCHED AND SRC.DepartmentDesc <> TRG.DepartmentDesc

	THEN UPDATE SET DepartmentDesc = SRC.DepartmentDesc

	WHEN NOT MATCHED THEN INSERT (DepartmentDesc) VALUES (SRC.DepartmentDesc)
	
	WHEN NOT MATCHED BY SOURCE THEN DELETE;

------------------------------------------------------------------------
--	CREATE DIM_POSITION
------------------------------------------------------------------------
	
	IF OBJECT_ID('HumanResources..dim_Position') IS NULL

	BEGIN
		
		CREATE TABLE dim_Position (
			PositionID				SMALLINT	NOT NULL IDENTITY(1,1) 
		,	PositionDesc			VARCHAR(50)
		,   DepartmentID			SMALLINT	NOT NULL
		
			CONSTRAINT PK_Position PRIMARY KEY (PositionID)
			CONSTRAINT FK_Department FOREIGN KEY (DepartmentID)
									 REFERENCES dim_Department(DepartmentID)
		)

	END

-- LOADS THE DATA INTO THE DIM_POSITION
---------------------------------------

	MERGE dim_Position TRG
	USING (
			SELECT DISTINCT
				B.PositionID
			,	A.PositionDesc
			,	CASE 
					WHEN C.DepartmentID <> 1 AND A.PositionDesc = 'Accountant I'
						THEN  1 
					ELSE C.DepartmentID
				END AS DepartmentID
			FROM ##TMP_HR A
			
			LEFT
			JOIN dim_Position B
			ON A.PositionDesc = B.PositionDesc

			LEFT 
			JOIN dim_Department C
			ON A.DepartmentDesc = C.DepartmentDesc 

		) SRC

		ON  SRC.PositionDesc = TRG.PositionDesc

	WHEN MATCHED AND (TRG.PositionDesc <> SRC.PositionDesc OR TRG.DepartmentID <> SRC.DepartmentID) THEN
		UPDATE SET PositionDesc = SRC.PositionDesc, 
				   DepartmentID = SRC.DepartmentID

	WHEN NOT MATCHED BY TARGET 
		THEN INSERT (PositionDesc, DepartmentID) VALUES (SRC.PositionDesc, SRC.DepartmentID)
	
	WHEN NOT MATCHED BY SOURCE THEN 
		DELETE;

------------------------------------------------------------------------
--	CREATE DIM_MARITALSTATUS
------------------------------------------------------------------------

	IF OBJECT_ID('HumanResources..dim_MaritalStatus') IS NULL

	BEGIN

		CREATE TABLE dim_MaritalStatus (
			MaritalStatusID		SMALLINT IDENTITY(1,1) NOT NULL
		,	MaritalStatusDesc	VARCHAR(15)

		CONSTRAINT PK_MaritalStatus PRIMARY KEY (MaritalStatusID)
		)

	END

-- LOADS THE DATA INTO THE DIM_MARITALSTATUS
--------------------------------------------

	MERGE dim_MaritalStatus TRG
	USING (
			SELECT DISTINCT
				B.MaritalStatusID
			,	A.MaritalStatusDesc
			FROM ##TMP_HR A
	
			LEFT
			JOIN dim_MaritalStatus B
			ON A.MaritalStatusDesc = B.MaritalStatusDesc
			) SRC
	
			ON	TRG.MaritalStatusDesc = SRC.MaritalStatusDesc
	
	WHEN MATCHED AND TRG.MaritalStatusDesc <> SRC.MaritalStatusDesc THEN
		UPDATE SET TRG.MaritalStatusDesc = SRC.MaritalStatusDesc
	
	WHEN NOT MATCHED BY TARGET THEN
		 INSERT (MaritalStatusDesc) VALUES (SRC.MaritalStatusDesc)
	
	WHEN NOT MATCHED BY SOURCE THEN
		DELETE;


------------------------------------------------------------------------
--	CREATE DIM_SHIFT
------------------------------------------------------------------------

	IF OBJECT_ID('HumanResources..dim_Shift') IS NULL
	
	BEGIN

		CREATE TABLE dim_Shift (
			ShiftID				SMALLINT IDENTITY(1,1) NOT NULL
		,	ShiftDesc			VARCHAR(15)
		,	ShiftStartTime		TIME
		,	ShiftEndTime		TIME

		CONSTRAINT PK_Shift PRIMARY KEY (ShiftID)
		)

	END

-- LOADS THE DATA INTO THE DIM_SHIFT
--------------------------------------------

	MERGE dim_Shift TRG
	USING (
			SELECT DISTINCT
				B.ShiftID
			,	A.ShiftDesc
			,	CASE 
					WHEN A.ShiftDesc = 'First Shift'  THEN '07:00:00'
					WHEN A.ShiftDesc = 'Second Shift' THEN '15:00:00'
					WHEN A.ShiftDesc = 'Third Shift'  THEN '23:00:00'
				END AS ShiftStartTime
			,	CASE 
					WHEN A.ShiftDesc = 'First Shift'  THEN '15:00:00'
					WHEN A.ShiftDesc = 'Second Shift' THEN '23:00:00'
					WHEN A.ShiftDesc = 'Third Shift'  THEN '07:00:00'
				END AS ShiftEndTime
			FROM ##TMP_HR A

			LEFT
			JOIN dim_Shift B
			ON A.ShiftDesc = B.ShiftDesc
			
			) SRC

			ON TRG.ShiftDesc = SRC.ShiftDesc

	WHEN MATCHED AND (   TRG.ShiftDesc		<> SRC.ShiftDesc 
	                  OR TRG.ShiftStartTime <> SRC.ShiftStartTime 
					  OR TRG.ShiftEndTime	<> SRC.ShiftEndTime)  THEN
		UPDATE SET ShiftDesc = SRC.ShiftDesc
				,  ShiftStartTime = SRC.ShiftStartTime
				,  ShiftEndTime = SRC.ShiftEndTime

	WHEN NOT MATCHED BY TARGET THEN
		INSERT (ShiftDesc, ShiftStartTime, ShiftEndTime ) VALUES (SRC.ShiftDesc, SRC.ShiftStartTime, SRC.ShiftEndTime)

	WHEN NOT MATCHED BY SOURCE THEN
		DELETE;

------------------------------------------------------------------------
--	CREATE DIM_RACE
------------------------------------------------------------------------

	IF OBJECT_ID('HumanResources..dim_Race') IS NULL
	
	BEGIN

		CREATE TABLE dim_Race (
			RaceID		SMALLINT IDENTITY(1,1) NOT NULL
		,	RaceDesc	VARCHAR(50)

		CONSTRAINT PK_Race PRIMARY KEY (RaceID)
		)

	END

-- LOADS THE DATA INTO THE DIM_RACE
--------------------------------------------

	MERGE dim_Race TRG
	USING (
			SELECT DISTINCT
				B.RaceID
			,	A.RaceDesc
			FROM ##TMP_HR A

			LEFT
			JOIN dim_Race B
			ON   A.RaceDesc = B.RaceDesc
		  ) SRC

			ON	TRG.RaceDesc = SRC.RaceDesc

	WHEN MATCHED AND TRG.RaceDesc <> SRC.RaceDesc THEN
		UPDATE SET RaceDesc = SRC.RaceDesc

	WHEN NOT MATCHED BY TARGET THEN
		INSERT (RaceDesc) VALUES (SRC.RaceDesc)

	WHEN NOT MATCHED BY SOURCE THEN
		DELETE;

------------------------------------------------------------------------
--	CREATE DIM_SITUATION
------------------------------------------------------------------------

	IF OBJECT_ID('HumanResources..dim_Situation') IS NULL
	
	BEGIN

		CREATE TABLE dim_Situation (
			SituationID		SMALLINT IDENTITY(1,1) NOT NULL
		,	SituationDesc	VARCHAR(50)

		CONSTRAINT PK_Situation PRIMARY KEY (SituationID)
		)

	END

-- LOADS THE DATA INTO THE DIM_SITUATION
--------------------------------------------

	MERGE dim_Situation TRG
	USING (
			SELECT DISTINCT
				B.SituationID
			,	A.SituationDesc
			FROM ##TMP_HR A

			LEFT
			JOIN dim_Situation B
			ON   A.SituationDesc = B.SituationDesc
		  ) SRC

			ON	TRG.SituationDesc = SRC.SituationDesc

	WHEN MATCHED AND TRG.SituationDesc <> SRC.SituationDesc THEN
		UPDATE SET SituationDesc = SRC.SituationDesc

	WHEN NOT MATCHED BY TARGET THEN
		INSERT (SituationDesc) VALUES (SRC.SituationDesc)

	WHEN NOT MATCHED BY SOURCE THEN
		DELETE;

------------------------------------------------------------------------
--	CREATE DIM_CITIZENSHIP
------------------------------------------------------------------------

	IF OBJECT_ID('HumanResources..dim_Citizenship') IS NULL
	
	BEGIN
		
		CREATE TABLE dim_Citizenship (
			CitizenshipID		SMALLINT IDENTITY(1,1) NOT NULL
		,	CitizenshipDesc		VARCHAR(50)

		CONSTRAINT PK_Citizenship PRIMARY KEY (CitizenshipID)
		)

	END

-- LOADS THE DATA INTO THE DIM_CITIZENSHIP
--------------------------------------------

	MERGE dim_Citizenship TRG
	USING (
			SELECT DISTINCT
				B.CitizenshipID
			,	A.CitizenshipDesc
			FROM ##TMP_HR A

			LEFT
			JOIN dim_Citizenship B
			ON   A.CitizenshipDesc = B.CitizenshipDesc
		  ) SRC

			ON	TRG.CitizenshipDesc = SRC.CitizenshipDesc

	WHEN MATCHED AND TRG.CitizenshipDesc <> SRC.CitizenshipDesc THEN
		UPDATE SET CitizenshipDesc = SRC.CitizenshipDesc

	WHEN NOT MATCHED BY TARGET THEN
		INSERT (CitizenshipDesc) VALUES (SRC.CitizenshipDesc)

	WHEN NOT MATCHED BY SOURCE THEN
		DELETE;

------------------------------------------------------------------------
--	CREATE DIM_CONTRACTTYPE
------------------------------------------------------------------------

	IF OBJECT_ID('HumanResources..dim_ContractType') IS NULL
	
	BEGIN

		CREATE TABLE dim_ContractType (
			ContractTypeID		SMALLINT IDENTITY(1,1) NOT NULL
		,	ContractTypeDesc	VARCHAR(50)

		CONSTRAINT PK_ContractType PRIMARY KEY (ContractTypeID)
		)

	END

-- LOADS THE DATA INTO THE DIM_CONTRACTTYPE
--------------------------------------------

	MERGE dim_ContractType TRG
	USING (
			SELECT DISTINCT
				B.ContractTypeID
			,	A.ContractTypeDesc
			FROM ##TMP_HR A

			LEFT
			JOIN dim_ContractType B
			ON   A.ContractTypeDesc = B.ContractTypeDesc
		  ) SRC

			ON	TRG.ContractTypeDesc = SRC.ContractTypeDesc

	WHEN MATCHED AND TRG.ContractTypeDesc <> SRC.ContractTypeDesc THEN
		UPDATE SET ContractTypeDesc = SRC.ContractTypeDesc

	WHEN NOT MATCHED BY TARGET THEN
		INSERT (ContractTypeDesc) VALUES (SRC.ContractTypeDesc)

	WHEN NOT MATCHED BY SOURCE THEN
		DELETE;


------------------------------------------------------------------------
--	CREATE FACT_EMPLOYEE
------------------------------------------------------------------------

	IF OBJECT_ID('HumanResources..fact_Employee') IS NULL

	
		CREATE TABLE fact_Employee (
			EmpID			INT IDENTITY(1,1) NOT NULL
		,	[Name]			VARCHAR(50)
		,	Sex			CHAR(1)
		,	BirthDate		DATE
		,	Age			AS ( FLOOR( DATEDIFF( DAY, CONVERT(DATE,BirthDate,103), GETDATE() ) / 365.25 ) )
		,	AgeGroup		AS ( CASE WHEN ( FLOOR( DATEDIFF( DAY, BirthDate, GETDATE() ) / 365.25 ) ) BETWEEN 15 AND 19 THEN '15-19' 
										  WHEN ( FLOOR( DATEDIFF( DAY, BirthDate, GETDATE() ) / 365.25 ) ) BETWEEN 20 AND 29 THEN '20-29' 
										  WHEN ( FLOOR( DATEDIFF( DAY, BirthDate, GETDATE() ) / 365.25 ) ) BETWEEN 30 AND 39 THEN '30-39'
										  WHEN ( FLOOR( DATEDIFF( DAY, BirthDate, GETDATE() ) / 365.25 ) ) BETWEEN 40 AND 49 THEN '40-49'
										  WHEN ( FLOOR( DATEDIFF( DAY, BirthDate, GETDATE() ) / 365.25 ) ) BETWEEN 50 AND 59 THEN '50-59'
										  WHEN ( FLOOR( DATEDIFF( DAY, BirthDate, GETDATE() ) / 365.25 ) ) BETWEEN 60 AND 69 THEN '60-69'
										  WHEN ( FLOOR( DATEDIFF( DAY, BirthDate, GETDATE() ) / 365.25 ) ) BETWEEN 70 AND 79 THEN '70-79'
										  WHEN ( FLOOR( DATEDIFF( DAY, BirthDate, GETDATE() ) / 365.25 ) )>= 80 THEN '>80'
										  ELSE 'Not Informed'
									END ) 
		,	MaritalStatusID		SMALLINT	NOT NULL REFERENCES dim_MaritalStatus	(MaritalStatusID)
		,	PositionID		SMALLINT	NOT NULL REFERENCES dim_Position		(PositionID)
		,	DepartmentID		SMALLINT	NOT NULL REFERENCES dim_Department		(DepartmentID)
		,	ShiftID			SMALLINT	NOT NULL REFERENCES dim_Shift			(ShiftID)
		,	CitizenshipID		SMALLINT	NOT NULL REFERENCES	dim_Citizenship		(CitizenshipID)
		,	ContractTypeID		SMALLINT	NOT NULL REFERENCES dim_ContractType	(ContractTypeID)
		,	RaceID			SMALLINT	NOT NULL REFERENCES dim_Race			(RaceID)
		,	Salary			FLOAT	
		,	HireDate		DATE
		,	TerminationDate		DATE
		,	SituationID		SMALLINT	NOT NULL REFERENCES dim_Situation		(SituationID)
		,	TerminationReason	VARCHAR(1000)
		,	RetentionDays		SMALLINT
		,	RetentionDaysGroup	AS (
									CASE WHEN RetentionDays < 60               THEN 'Less than 60 days'
										 WHEN RetentionDays BETWEEN 60 AND 365 THEN 'Between 60 and 365 days'
										 WHEN RetentionDays > 365              THEN 'Greater than 365 days'
									END )

		,	BadHiring AS ( CASE WHEN RetentionDays < 60 AND SituationID = 4 --Fired
	                          THEN 1
							  ELSE 0
						 END )
		)





	





















			
