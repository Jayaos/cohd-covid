## Introduction

This repository has source codes for reproducing methods and analyses in the following [paper](https://scholar.google.com/citations?user=iSx6QrwAAAAJ&hl=en&oi=ao):

    COHD-COVID: Columbia Open Health Data for COVID-19 Research
        Junghwan Lee, Jae Hyun Kim, Cong Liu, Daichi Shimbo, Marwah Abdalla, Casey Ta, Chunhua Weng
        Preprint

### To Export Raw Data from OMOP Database

To export raw patient data from OMOP database, execute the query of the cohort you wanted to export in /sql_queries directory. Concept definitions can be exported by executing export_concepts.sql in the /sql_queries/settings directory.

### To Calculate Concept Count, Concept Co-occurrence, and Symptom Prevalence

analysis.py contain functions to calculate the concept count, concept co-occurrence, and symptom prevalence based on the raw data exported from the database. prevalence_example.ipynb contain examples to use the functions in the package.

### Analyses using Concept Count, Concept Co-occurrence, and Symptom Prevalence

analysis.py contain functions to perform various analyses based on the concept count, concept co-occurrence, and symptom prevalence results. analysis_example.ipynb contain examples to use the functions and to perfrom analyses in the paper.