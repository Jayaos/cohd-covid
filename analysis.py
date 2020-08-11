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
            count_dict[int(concept_id)] = count
            prev_dict[int(concept_id)] = count / num_people
    
    return count_dict, prev_dict

def get_prevalence_rank(prevalence_dict, concept_dict, top_n, domain):

    if domain == "condition":
        target_concept_dict = concept_dict["condition_dict"]
    elif domain == "drug":
        target_concept_dict = concept_dict["drug_dict"]
    elif domain == "drug_ingredient":
        target_concept_dict = concept_dict["drug_ingredient_dict"]
    elif domain == "procedure":
        target_concept_dict = concept_dict["procedure_dict"]

    concepts = list(set.intersection(set(target_concept_dict.keys()), set(prevalence_dict.keys())))
    prevalences = []

    for concept in concepts:
        prevalences.append(prevalence_dict[concept])

    sorted_idx = np.array(prevalences).argsort()
    sorted_n_concepts = concepts[sorted_idx][-(top_n):]
    sorted_n_prevalences = prevalences[sorted_idx][-(top_n):]

    for i in range(top_n):
        print("Rank {order} : {concept_name}, {prevalence}".format(order=(i+1), 
        concept_name=concept_dict[str(sorted_n_concepts[-(i+1)])],
        prevalence=sorted_n_prevalences[-(i+1)]))

def build_concept_dict(concepts, database="ssms"):

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
            total_dict[int(concept_id)] = [concept_name, domain_id]

            if domain_id == "Condition":
                condition_dict[int(concept_id)] = [concept_name, domain_id]
            if domain_id == "Drug":
                concept_id, concept_name, domain_id, vocabulary_id, concept_class_id = [row[i] for i in columns_drug]
                drug_dict[int(concept_id)] = [concept_name, domain_id, vocabulary_id, concept_class_id]
                if concept_class_id == "Ingredient":
                    drug_ingredient_dict[int(concept_id)] = [concept_name, domain_id, vocabulary_id, concept_class_id]
    
    concept_definition_dict["condition_dict"] = condition_dict
    concept_definition_dict["drug_dict"] = drug_dict
    concept_definition_dict["drug_ingredient_dict"] = drug_ingredient_dict
    concept_definition_dict["total_dict"] = total_dict

    return concept_definition_dict

def build_ratio_dict(covid_prevalence_dict, baseline_prevalence_dict, concept_dict):
    prevalence_ratio_dict = dict()
    
    covid_concepts = set(covid_prevalence_dict.keys())
    baseline_concepts = set(baseline_prevalence_dict.keys())
    intersection_concepts = set.intersection(covid_concepts, baseline_concepts)

    condition_intersection_concepts = set.intersection(set(concept_dict["condition_dict"].keys()), intersection_concepts)
    drug_intersection_concepts = set.intersection(set(concept_dict["drug_dict"].keys()), intersection_concepts)
    drug_ingredient_intersection_concepts = set.intersection(set(concept_dict["drug_ingredient_dict"].keys()), intersection_concepts)
    procedure_intersection_concepts = set.intersection(set(concept_dict["procedure_dict"].keys()), intersection_concepts)

    condition_ratio_dict = dict()
    drug_ratio_dict = dict()
    drug_ingredient_ratio_dict = dict()
    procedure_ratio_dict = dict()

    print("build prevalence ratio dictionary for condition concepts...")
    for concept in list(condition_intersection_concepts):
        condition_ratio_dict[int(concept)] = np.log(covid_freqdict[concept] / (baseline_freqdict[concept]))

    print("build prevalence ratio dictionary for drug concepts...")
    for concept in list(drug_intersection_concepts):
        drug_ratio_dict[int(concept)] = np.log(covid_freqdict[concept] / (baseline_freqdict[concept]))

    print("build prevalence ratio dictionary for drug ingredient concepts...")
    for concept in list(drug_ingredient_intersection_concepts):
        drug_ingredient_ratio_dict[int(concept)] = np.log(covid_freqdict[concept] / (baseline_freqdict[concept]))

    print("build prevalence ratio dictionary for procedure concepts...")
    for concept in list(procedure_intersection_concepts):
        procedure_ratio_dict[int(concept)] = np.log(covid_freqdict[concept] / (baseline_freqdict[concept]))

    prevalence_ratio_dict["condition_ratio_dict"] = condition_ratio_dict
    prevalence_ratio_dict["drug_ratio_dict"] = drug_ratio_dict
    prevalence_ratio_dict["drug_ingredient_ratio_dict"] = drug_ingredient_ratio_dict
    prevalence_ratio_dict["procedure_ratio_dict"] = procedure_ratio_dict

    return prevalence_ratio_dict

def get_ratio_rank(prevalence_ratio_dict, top_n, domain):
    """Get the top-n highest concepts from the ratio dictionary for a specific domain"""

    if domain == "condition":
        target_dict = prevalence_ratio_dict["condition_ratio_dict"]
    elif domain == "drug":
        target_dict = prevalence_ratio_dict["drug_ratio_dict"]
    elif domain == "drug_ingredient":
        target_dict = prevalence_ratio_dict["drug_ingredient_ratio_dict"]
    elif domain == "procedure":
        target_dict = prevalence_ratio_dict["procedure_ratio_dict"]

    concepts = list(target_dict.keys())
    ratios = []

    for concept in concetps:
        ratios.append(target_dict[concept])

    sorted_idx = np.array(ratios).argsort()
    sorted_n_concepts = concepts[sorted_idx][-(top_n):]
    sorted_n_ratios = ratios[sorted_idx][-(top_n):]

    for i in range(top_n):
        print("Rank {order} : {concept_name}, {prevalence}".format(order=(i+1), 
        concept_name=concept_dict[str(sorted_n_concepts[-(i+1)])],
        prevalence=sorted_n_ratios[-(i+1)]))

def build_pair_prevalence_dict(concept_pair, denominator, database="ssms"):

    fh, reader = _open_csv_reader(concept_pair, database)

    header = next(reader)
    table_width = len(header)
    columns = _find_columns(header, ["concept_id1", "concept_id2", "count"])

    pair_prevalence_dict = defaultdict(dict)
    
    for row in reader:
        if len(row) == table_width:
            concept_id1, concept_id2, count = [row[i] for i in columns]
            pair_prevalence_dict[int(concept_id1)][int(concept_id2)] = int(count) / denominator
            pair_prevalence_dict[int(concept_id2)][int(concept_id1)] = int(count) / denominator

    return pair_prevalence_dict

def get_pair_rank(pair_prevalence_dict, concept_dict, candidate_concept):

    paired_concepts = list(pair_prevalence_dict[candidate_concept].keys())

    concepts = []
    prevalences = []

    for concept in paired_concepts:
        concepts.append(concept)
        prevalences.append(pair_prevalence_dict[candidate_concept][concept])

    sorted_idx = np.array(prevalences).argsort()
    sorted_concepts = concepts[sorted_idx]
    sorted_prevalences = prevalences[sorted_idx]

    for i in range(len(sorted_concepts)):
        print("Rank {order} : {concept_name}, {prevalence}".format(order=(i+1), 
        concept_name=concept_dict[str(sorted_concepts[-(i+1)])],
        prevalence=sorted_prevalences[-(i+1)]))

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