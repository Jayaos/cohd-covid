import csv
import logging
import os
import datetime

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