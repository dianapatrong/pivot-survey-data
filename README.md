# SE-SQL-Assignment

#### This project is based on using programmatic access to the database server to replicate a stored procedure to build dynamic SQL pivot survey answers data in usable format for analysis in the database `Survey_Sample_A19`. It replicates the following: 

1. A stored function `dbo.fn_GetAllSurveyDataSQL()` which generates and
returns a dynamic SQL query string for extracting the pivoted survey answer data

2. A trigger `dbo.trg_refreshSurveyView`
    * a. firing on INSERT, DELETE and UPDATE upon the table dbo.SurveyStructure
    * b. executing a CREATE OR ALTER VIEW vw_AllSurveyData AS + the string returned by dbo.fn_GetAllSurveyDataSQL
    
    
For achieving this I have use the following: 
* `current_survey_structure.csv`  which stores the last known survey's structure in a csv file
* `fresh_survey_data.csv`  which stores the "always-fresh" pivoted survey data in a csv file



In order to run this we must create the file `db_config.yml` in the parent folder and substitute it with the actual values to be used:

```
driver: '<driver>'
server: '<server>'
database: '<database>'
uid: '<user_id>'
pwd: '<password>'
```