# SE-SQL-Assignment

#### This project is based on using programmatic access to the database server to replicate a stored procedure to build dynamic SQL pivot survey answers data in usable format for analysis in the database `Survey_Sample_A19`. It replicates the following: 

1. A stored function `dbo.fn_GetAllSurveyDataSQL()` which generates and
returns a dynamic SQL query string for extracting the pivoted survey answer data

2. A trigger `dbo.trg_refreshSurveyView`
    * a. firing on INSERT, DELETE and UPDATE upon the table dbo.SurveyStructure
    * b. executing a CREATE OR ALTER VIEW vw_AllSurveyData AS + the string returned by dbo.fn_GetAllSurveyDataSQL
    
    
For achieving this I have used the following files: 
* `db_executor.py` which handles the connection to the database server.
* `get_all_survey_data.py` which replicates the algorithm of `dbo.fn_GetAllSurveyDataSQL()` and triggers and update of the view `dbo.trg_refreshSurveyView`
* `survey_structure.csv`  which stores the last known survey's structure in a csv file
* `fresh_survey_data.csv`  which stores the _"always-fresh"_ pivoted survey data in a csv file



In order to run the code first we must create the file `db_config.yml` outside of the folder of the project and substitute it with the actual values to be used:

```
driver: '<driver>'
server: '<server>'
database: '<database>'
uid: '<user_id>'
pwd: '<password>'
```

Then we can run the file **`main.py`** which will handle the installation of the dependencies (listed in `requirements.txt`)

```
python3 main.py
```