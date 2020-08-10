import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from collections import defaultdict, OrderedDict
import codecs
import csv

def build_prevalence_dict(concept_count, num_people, database="ssms"):
    
    fh, reader = _open_csv_reader(concept_count, database)
    
    header = next(reader)
    table_width = len(header)
    columns = _find_columns(header, ["concept_id", "count"])

    count_dict = dict()
    prev_dict = dict()

    for row in reader:
        if len(row) == table_width:
            concept_id, count = [row[i] for i in columns]
            count = int(count)
            count_dict[concept_id] = count
            prev_dict[concept_id] = count / num_people
    
    return count_dict, prev_dict

def get_prevalence_rank():
    None

def build_concept_dict(concepts):

    fh, reader = _open_csv_reader(concepts, database)
    
    header = next(reader)
    table_width = len(header)
    columns = _find_columns(header, ["concept_id", "concept_name", "domain_id"])
    columns_drug = _find_columns(header, ["concept_id", "concept_name", "domain_id", "vocabulary_id", "concept_class_id"])

    concept_definition_dict = dict()   
    condition_dict = dict()
    drug_dict = dict()
    drug_ingredient_dict = dict()
    procedure_dict = dict()
    total_dict = dict()

    for row in reader:
        if len(row) == table_width:
            concept_id, concept_name, domain_id = [row[i] for i in columns]
            total_dict[concept_id] = [concept_name, domain_id]

            if domain_id == "Condition":
                condition_dict[concept_id] = [concept_name, domain_id]
            if domain_id == "Drug":
                concept_id, concept_name, domain_id, vocabulary_id, concept_class_id = [row[i] for i in columns_drug]
                drug_dict[concept_id] = [concept_name, domain_id, vocabulary_id, concept_class_id]
                if concept_class_id == "Ingredient":
                    drug_ingredient_dict[concept_id] = [concept_name, domain_id, vocabulary_id, concept_class_id]
    
    concept_dict["condition_dict"] = condition_dict
    concept_dict["drug_dict"] = drug_dict
    concept_dict["drug_ingredient_dict"] = drug_ingredient_dict
    concept_dict["total_dict"] = total_dict

    return concept_dict

def build_ratio_dict(covid_prevalence_dict, baseline_prevalence_dict, concept_dict):
    None

def get_ratio_rank():
    None

def build_pair_dict(concept_pair, denominator):
    None

def get_pair_rank():
    None

def _open_csv_reader(file, database):
    """Opens a CSV reader compatible with the specified database
    Microsoft SQL Server Management Studio (SSMS) exports CSV files in unicode. Python's native CSV reader can't handle
    unicode. Convert to UTF-8 to read. This is noticeably slower than using the native reader, so an alternative
    solution is to re-write SSMS output using a text editor like Sublime prior to running the Python scripts.
    
    Parameters
    ----------
    file: string - file name 
    database: string - database which the file was generated from
        "ssms" - SQL Server Management Studio
        "mysql" - MySQL
    """
    if database == "ssms":
        # Microsoft SQL Server Management Studio output
        fh = codecs.open(file, 'r', encoding='utf-8-sig')  
        reader = _unicode_csv_reader(fh, delimiter='\t')
    else:
        # Unknown database type. Just try opening as regular
        fh = open(file) 
        reader = csv.reader(fh, delimiter='\t') 
    return fh, reader

def _unicode_csv_reader(unicode_csv_data, dialect=csv.excel, **kwargs):
    """
    Read a CSV file encoded in Unicode
    The native csv.reader does not read Unicode. 
    Encode the data source as UTF-8 and decode it
    """
    return csv.reader(codecs.iterdecode(_utf_8_encoder(unicode_csv_data), "utf-8"), dialect=csv.excel, **kwargs)

def _utf_8_encoder(unicode_csv_data):
    """Encodes Unicode source as UTF-8"""
    for line in unicode_csv_data:
        yield line.encode('utf-8')

def _find_columns(header, column_names):
    """Finds the index of the column names in the header"""
    return [[i for i in range(len(header)) if header[i] == column_name][0] for column_name in column_names]