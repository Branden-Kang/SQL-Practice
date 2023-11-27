CREATE PROCEDURE procedure_name
AS
sql_query or sql_statement
GO;

EXEC procedure_name;  --SQL SERVER, ORACLE
CALL procedure_name;  --Postgre SQL, MySQL
DROP  PROCEDURE procedure_name;

-- Example
CREATE TABLE Persons_Store_Proceduree (
    PersonID int IDENTITY(1,1) PRIMARY KEY ,
    FirstName varchar(255),
    LastName varchar(255),
    Address varchar(255),
    City varchar(255),
    Dates DATE,
    DailyMetrics varchar(255)  --for one week.
);

INSERT INTO Persons_Store_Proceduree 
VALUES (1,"Branden", "Kang", "Manhattan", "NY"," 2023-11-27","100 points"); 

SELECT * FROM Persons_Store_Proceduree;

CREATE PROCEDURE Metric_Show_Procedure_For_Persons
AS
SELECT * FROM Persons_Store_Proceduree;
GO;

--EXECUTING TABLE
CALL Metric_Show_Procedure_For_Persons;

CREATE PROCEDURE Select_Data_As_City @City VARCHAR(255)
AS
SELECT * FROM Metric_Show_Procedure_For_Persons
WHERE City=@City
;
GO;

EXEC Select_Data_As_City @City='NY';

