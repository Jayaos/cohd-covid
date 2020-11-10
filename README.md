## Introduction

This repository has source codes for reproducing methods and analyses in the following [paper](https://scholar.google.com/citations?user=iSx6QrwAAAAJ&hl=en&oi=ao):

    COHD-COVID: Columbia Open Health Data for COVID-19 Research
        Junghwan Lee, Jae Hyun Kim, Cong Liu, George Hripcsak, Casey Ta, Chunhua Weng
        Preprint

### To Export Raw Data from OMOP Database via SQL Server Management Studio (SSML)

1. Update settings in SQL Server Manangement Studio so that Results to Text saves tab-delimited files
at Tools > Options > Query Results > SQL Server > Results to Text
-Output format: tab delimited
-Include column headers in the result set: enabled
Then restart SSMS for new settings to take effect
2. Enable SQLCMD mode at Query > SQLCMD Mode
3. Execute the query you wanted to export in /sql_queries directory with updated output path after :OUT command.

### To Calculate Concept Count, Concept Co-occurrence, and Symptom Prevalence

analysis.py contain functions to calculate the concept count, concept co-occurrence, and symptom prevalence based on the raw data exported from the database. prevalence_example.ipynb contain examples to use the functions in the package.

### Analyses using Concept Count, Concept Co-occurrence, and Symptom Prevalence

analysis.py contain functions to perform various analyses based on the concept count, concept co-occurrence, and symptom prevalence results. analysis_example.ipynb contain examples to use the functions and to perfrom analyses in the paper.