import csv
import logging
import os
import datetime
import codecs
from collections import defaultdict
from collections import namedtuple

def strip_hypen(my_date):
    splitted = my_date.split("-")
    year = int(splitted[0])
    return year, "".join(splitted)

def logging_setup(output_dir):
    """ Set up for logging to log to file and to stdout
    Log file will be named by current time
    Parameters
    ----------
    output_dir: string - Location to create log file
    """
    # Set up logger to print to file and stream
    log_formatter = logging.Formatter("%(asctime)s %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # File log
    log_file = 'log_' + datetime.datetime.now().strftime("%Y-%m-%d_%H%M") + '.txt'
    log_file = os.path.join(output_dir, log_file)
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)

    # Stream log
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)

def _find_columns(header, column_names):
    """Finds the index of the column names in the header"""
    return [[i for i in range(len(header)) if header[i] == column_name][0] for column_name in column_names]

def _utf_8_encoder(unicode_csv_data):
    """Encodes Unicode source as UTF-8"""
    for line in unicode_csv_data:
        yield line.encode('utf-8')

def _unicode_csv_reader(unicode_csv_data, dialect=csv.excel, **kwargs):
    """
    Read a CSV file encoded in Unicode
    The native csv.reader does not read Unicode. 
    Encode the data source as UTF-8 and decode it
    """
    return csv.reader(codecs.iterdecode(_utf_8_encoder(unicode_csv_data), "utf-8"), dialect=csv.excel, **kwargs)

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
    # else:
        # Unknown database type. Just try opening as regular
        # logging.info('_open_csv_reader - Unknown database')
        # fh = open(file) 
        # reader = csv.reader(fh, delimiter='\t') 
    return fh, reader

def load_concepts(file, database, extra_header_lines_skip=0):
    """Load concept definitions
    
    Parameters
    ----------
    file: string - Concepts data file
    database: string - Originating database. See _open_csv_reader
    extra_header_lines_skip - int - Number of lines to skip after the header
    
    Returns
    -------
    Dictionary[concept_id] -> Dictionary, keys: {concept_name, domain_id, concept_class_id}
    """
    logging.info("Loading concepts...")
    
    # Open csv reader
    fh, reader = _open_csv_reader(file, database)

    # Read header
    header = next(reader)
    table_width = len(header)
    if table_width == 4:
        columns = _find_columns(header, ['concept_id', 'concept_name', 'domain_id', 'concept_class_id'])
    elif table_width > 5:
        columns = _find_columns(header,
                                ['concept_id', 'concept_name', 'domain_id', 'vocabulary_id', 'concept_class_id'])


    # Skip extra formatting lines after header
    for i in range(extra_header_lines_skip):
        next(reader)

    # Read in each row of the file
    concepts = dict()
    for row in reader:
        if len(row) == table_width:
            if table_width == 4:
                concept_id, concept_name, domain_id, concept_class_id = [row[i] for i in columns]
                # Convert concept_id to int
                concept_id = int(concept_id)
                concepts[concept_id] = {'concept_name': concept_name,
                                        'domain_id': domain_id,
                                        'concept_class_id': concept_class_id}
            elif table_width > 5:
                concept_id, concept_name, domain_id, vocabulary_id, concept_class_id = [row[i] for i in columns]
                # Convert concept_id to int
                concept_id = int(concept_id)
                concepts[concept_id] = {'concept_name': concept_name,
                                        'domain_id': domain_id,
                                        'vocabulary_id': vocabulary_id,
                                        'concept_class_id': concept_class_id}

    logging.info("%d concept definitions loaded" % len(concepts))
    
    fh.close()
    return concepts

def load_descendants(file, database, extra_header_lines_skip=0):
    """Load each concept's direct descendants
    Parameters
    ----------
    file: string - Descendants data file
    database: string - Originating database. See _open_csv_reader
    extra_header_lines_skip: int - Number of lines to skip after the header
    Returns
    -------
    Dictionary[concept_id] -> set(concept_ids)
    """
    logging.info('Loading descendants...')

    # Open csv reader
    fh, reader = _open_csv_reader(file, database)

    # Read header
    header = next(reader)
    columns = _find_columns(header, ['concept_id', 'descendant_concept_id'])
    table_width = len(header)

    # Skip extra formatting lines after header
    for i in range(extra_header_lines_skip):
        next(reader)

    # Read in each row of the file and add the descendants to the dictionary
    concept_descendants = defaultdict(set)
    for row in reader:
        if len(row) == table_width:
            # Convert concept IDs to ints
            try:
              concept_id, descendant_concept_id = [int(row[i]) for i in columns]
            except:
              continue
            concept_descendants[concept_id].add(descendant_concept_id)

    fh.close()
    return concept_descendants

def load_patient_data(file, database, extra_header_lines_skip=0):
    """Load patient demographics data extracted from the OMOP person table
    
    Parameters
    ----------
    file: string - Patient data file
    database: string - Originating database. See _open_csv_reader
    extra_header_lines_skip - int - Number of lines to skip after the header
    
    Returns
    -------
    Dictionary[concept_id] -> [ethnicity, race, gender]
    """
    logging.info("Loading patient data...")

    # Open csv reader
    fh, reader = _open_csv_reader(file, database)
 
    # Read header line to get column names
    header = next(reader)
    columns = _find_columns(header, ['person_id', 'ethnicity_concept_id', 'race_concept_id', 'gender_concept_id'])
    table_width = len(header)

    # Skip extra formatting lines after header
    for i in range(extra_header_lines_skip):
        next(reader)

    # Read in each row
    patient_info = defaultdict(list)
    for row in reader:
        # Display progress
        if reader.line_num % 1000000 == 0: 
            logging.info(reader.line_num)
            
        if len(row) == table_width:
            # Get ethnicity, race, and gender and convert everything to ints (they're all person IDs or concept IDs)
            person_id, ethnicity, race, gender = [int(row[i]) for i in columns]
            patient_info[person_id] = [ethnicity, race, gender] 

    logging.info("%d persons loaded" % len(patient_info))
            
    fh.close()
    return patient_info

def load_concept_patient_data(file, database, patient_info, extra_header_lines_skip=0, iatrogenic_ids=set()):
    """Load concept-year-patient data
    
    Parameters
    ----------
    file: string - data file with concept_id, year, patient_id, and domain
    database: string - Originating database. See _open_csv_reader
    patient_info: object - Returned from load_patient_data
    extra_header_lines_skip - int - Number of lines to skip after the header
    
    Returns
    -------
    ConceptPatientData object
    """
    logging.info("Loading condition, drug, and procedure data...")
    
    # Create an empty namedtuple for recording data
    ConceptPatientData = namedtuple('ConceptPatientData', 
    ['concept_year_patient', 'year_patient', 'year_numpatients'])

    # Open csv reader
    fh, reader = _open_csv_reader(file, database)

    # Read header
    header = next(reader)
    columns = _find_columns(header, ['person_id', 'date', 'concept_id'])
    table_width = len(header)

    # Skip extra formatting lines after header
    for i in range(extra_header_lines_skip):
        next(reader)

    # Read in each row of the file
    concept_year_patient = defaultdict(lambda: defaultdict(set))
    year_patients = defaultdict(set)
    for row in reader:
        # Display progress
        if reader.line_num % 1000000 == 0:
            logging.info(reader.line_num)
            
        if len(row) == table_width:
            person_id, date_str, concept_id = [row[i] for i in columns]

            # Convert concept_id to int
            concept_id = int(concept_id)

            # Skip when concept_id is 0 or iatrogenic
            if concept_id == 0 or concept_id in iatrogenic_ids:
                continue
            
            # Create new person_date_id
            year, date_id = strip_hypen(date_str)
            person_date_id = person_id + "_" + date_id

            # Track concepts and patients by year
            concept_year_patient[concept_id][year].add(person_date_id)
            year_patients[year].add(person_date_id)

    # For each patient seen in each year, add the patient's demographics (race, ethnicity, gender)
    for year in year_patients:
        patients_in_year = year_patients[year]
        for person_date_id in patients_in_year:
            person_id = person_date_id.split("_")[0]
            pt_info = patient_info[person_id]
            for concept_id in pt_info:
                if concept_id != 0:
                    concept_year_patient[concept_id][year].add(person_id)                     
    
    # Count how many patients in each year
    year_numpatients = defaultdict(lambda: 0)
    for year, pts in year_patients.items():
        year_numpatients[year] = float(len(pts))
    
    logging.info("Loaded data for %d patients and %d concepts from %d rows." %
                 (len(patient_info), len(concept_year_patient), reader.line_num))

    fh.close()   
    return ConceptPatientData(concept_year_patient, year_patients, year_numpatients)
    
def merge_concepts_years(cp_data, year_min, year_max):
    """Merge data over the specified year range
    
    Parameters
    ----------
    cp_data: ConceptPatientData
    year_min: int - First year in the range (inclusive)
    year_max: int - Last year in the range (inclusive)
    
    Returns
    -------
    ConceptPatientDataMerged
    """
    # Create an empty namedtuple for recording data
    ConceptPatientDataMerged = namedtuple('ConceptPatientDataMerged', 
    ['concept_patient', 'patient', 'num_patients', 'year_min', 'year_max'])

    logging.info('Merging concepts in range %d - %d' % (year_min, year_max))
    
    # How often to display progress message
    concept_year_patient = cp_data.concept_year_patient
    n_concepts = len(concept_year_patient)
    progress_interval = round(n_concepts / 10)

    # Collect all patients for each concept across the range of years
    concepts_ranged = defaultdict(set)
    for counter, concept_id in enumerate(concept_year_patient):
        # Progress message
        if counter % progress_interval == 0:
            logging.info('%d%%' % round(counter / float(n_concepts) * 100))
            
        pts_merged = list()
        for year, pts in concept_year_patient[concept_id].items():
            # Skip if this is not in the desired year range
            if year < year_min or year > year_max:
                continue

            # Combine list of patients and remove duplicates later (more efficient)
            pts_merged.extend(pts)

        if len(pts_merged) > 0:
            concepts_ranged[concept_id] = set(pts_merged)
    
    # Merge the set of all patients across the years
    year_patient = cp_data.year_patient
    pts_merged = list()
    for year, pts in year_patient.items():
        if year >= year_min and year <= year_max:
            # Note: faster to concatenate lists and then convert to set later
            pts_merged.extend(year_patient[year])
    pts_merged = set(pts_merged)
    n_patients = float(len(pts_merged))

    logging.info('%d concepts, %d patients (this is the denominator for prevalence)' %
                 (len(concepts_ranged), n_patients))
        
    return ConceptPatientDataMerged(concepts_ranged, pts_merged, n_patients, year_min, year_max)

def merge_ranged_concept_descendants(cp_ranged, concepts, descendants):
    """Merge patients from descendant concepts.
    Run this after merging patients by date.
    Parameters
    ----------
    cp_ranged: ConceptPatientDataMerged
    concepts: Dictionary of all observed concepts and their ancestors. The concepts included in this dictionary identify
              which concepts to get merged patient sets for.
    descendants: Dictionary of each concept's descendants (all descendants at all levels)
    Returns
    -------
    ConceptPatientDataMerged
    """
    # Create a new namedtuple for recording data
    ConceptPatientDataMerged = namedtuple('ConceptPatientDataMerged', 
    ['concept_patient', 'patient', 'num_patients', 'year_min', 'year_max'])

    logging.info('Merging concepts hierarchically for %d-%d dataset' % (cp_ranged.year_min, cp_ranged.year_max))

    concept_patient = cp_ranged.concept_patient

    # Keep track of which concepts are finished.
    unfinished_concepts = set(concepts.keys())

    # Loop until we have merged all hierarchical concepts
    # Note: largest max_levels_of_separation in our OHDSI database is 24
    max_iterations = 50
    concepts_merged = defaultdict(set)
    for i in range(max_iterations):
        # Progress message
        n_unfinished_concepts = len(unfinished_concepts)
        logging.info('iteration %d: %d concepts remaining' % (i, n_unfinished_concepts))

        # How often to display progress message
        # progress_interval = round(n_unfinished_concepts / 10)	# Show progress every 10%
        progress_interval = 0  # Don't show progress

        # Keep track of which concepts were finished in this iteration
        newly_finished_concepts = set()

        # Merge patient sets for each concept if its descendants are finished
        for j, concept_id in enumerate(unfinished_concepts):
            # Progress message
            if (progress_interval > 0) and (j % progress_interval == 0):
                logging.info('%d%%' % round(j / float(n_unfinished_concepts) * 100))

            # Check if the descendants are finished
            descendants_finished = True
            descendant_ids = descendants[concept_id]
            for descendant_id in descendant_ids:
                if descendant_id not in concepts_merged:
                    descendants_finished = False
                    break

            if not descendants_finished:
                # This concept's descendants are not finished yet. Skip.
                continue

            # This concept's descendants are finished. Merge the patients with its descendants
            pts = list(concept_patient[concept_id])
            for descendant_id in descendant_ids:
                # Combine lists of patients now, remove duplicates later (more efficient)
                pts.extend(concepts_merged[descendant_id])

            # Save the set of unique patients and add this concept to the list of concepts finished in this iteration
            concepts_merged[concept_id] = set(pts)
            newly_finished_concepts.add(concept_id)

        # Update the set of unfinished concepts
        unfinished_concepts -= newly_finished_concepts

        # Check if we're finished
        if len(unfinished_concepts) == 0:
            # No more concepts to do. Exit the loop.
            break
        elif len(newly_finished_concepts) == 0:
            # Not done yet, but no new concepts were finished
            logging.warning('merge_concept_descendants: No new concepts finished')
        elif i == (max_iterations - 1):
            # Reached the max iterations without finishing. Notify the user.
            logging.warning('merge_concept_descendants: Terminated at max iterations without finishing')

    logging.info('merge_concept_descendants: finished with %d concepts, %d patients' %
                 (len(concepts_merged), len(cp_ranged.patient)))

    return ConceptPatientDataMerged(concepts_merged, cp_ranged.patient, cp_ranged.num_patients,
                                    cp_ranged.year_min, cp_ranged.year_max)   