from multiprocessing import Pool
from csv import writer, QUOTE_MINIMAL
import datetime
import pandas

""" Handles the population of the Crime table

Global Variables:
    GLOB_DENV_DATA: Contains the denver data
    GLOB_VAN_DATA: Contains the vancouver data

Categories:
    traffic-accident
    homicide
    mischief
    off-against-person
    white-collar-crime
    arson
    drug-alcohol
    all-other-crimes
    theft
    break-and-enter
"""

GLOB_DENV_DATA = None
GLOB_VAN_DATA = None


def gen_crime_csvs(van_data, denv_data):
    """ Creates two csvs, one for denver and one for vancouver
    This function should be run prior to loadCrime if the CSVs
    do not already exist

    """
    global GLOB_DENV_DATA
    global GLOB_VAN_DATA

    GLOB_DENV_DATA = denv_data
    GLOB_VAN_DATA = van_data

    # ------------------------- Handle Denver Data ----------------------------

    print("Crime dimension: Processing denver data...")
    with Pool() as pool:
        chunk_gen = pool.imap(handle_denv, denv_data.index, 16)
        # Store all chuncks from generator to list
        denv_list = [chunk for chunk in chunk_gen]
    print("Crime dimension: Done denver")

    # ------------------------- Handle Vancouver Data -------------------------

    print("Crime dimension: Processing vancouver data...")
    with Pool() as pool:
        chunk_gen = pool.imap(handle_van, van_data.index, 16)
        # Store all chunks from generator to list
        van_list = [chunk for chunk in chunk_gen]
    print("Crime dimension: Done vancouver")

    # ------------------------- Write to CSV -------------------------

    header = ["Crime_key", "Crime_report_time", "Crime_start_time", "Crime_end_time", "Crime_type", "Crime_category", "Crime_severity_index"]

    print("Crime dimension: Writing data to csv (crimeDim.csv)...")
    with open("../data/final/crimDim.csv", mode='w') as out_file:
        csv_writer = writer(out_file, delimiter=',', quotechar='"', quoting=QUOTE_MINIMAL)
        csv_writer.writerow(header)
        full_list = denv_list + van_list
        for index, row in enumerate(full_list):
            row.insert(0, index + 1)
            csv_writer.writerow(row)
    print("Crime dimension: Write complete")

def denv_parse_date(in_date):
    """ Process a date string from denver data and return corresponding datetime
    """
    if isinstance(in_date, str):
        date = datetime.datetime.strptime(in_date, "%m/%d/%Y %I:%M:%S %p")
    else:
        date = None
    return date


def van_map_offense(offCat):
    """ Perform category and type mapping for vancouver
    """
    if offCat == "Break and Enter Commercial":
        return "break-and-enter", "commercial", 1

    elif offCat == "Break and Enter Residential/Other":
        return "break-and-enter", "residential/other", 1

    elif offCat == "Homicide":
        return "homicide", "", 2

    elif offCat == "Mischief":
        return "mischief", "", 1

    elif offCat == "Offence Against a Person":
        return "off-against-person", "", 1

    elif offCat == "Other Theft":
        return "theft", "", 1

    elif offCat == "Theft from Vehicle":
        return "theft", "theft-from-auto", 1

    elif offCat == "Theft of Bicycle":
        return "theft", "theft-bicycle", 1

    elif offCat == "Theft of Vehicle":
        return "theft", "auto-theft", 1

    elif offCat == "Vehicle Collision or Pedestrian Struck (with Fatality)":
        return "traffic-accident", "traffic-accident-hit-and-run", 1

    elif offCat == "Vehicle Collision or Pedestrian Struck (with Injury)":
        return "traffic-accident", "traffic-accident-hit-and-run", 1

    else:
        print("No match!!", offCat)


def denv_map_offense(offCat, offType):
    """ Perform category and type mapping for vancouver
    """
    if offCat == "all-other-crimes":
        return "all-other-crimes", offType, 1

    elif offCat == "larceny" and offType == "theft-bicycle":
        return "theft", "theft-bicycle", 1

    elif offCat == "larceny":
        return "theft", offType, 1

    elif offCat == "theft-from-motor-vehicle":
        return "theft", "theft-from-auto", 1

    elif offCat == "traffic-accident":
        return "traffic-accident", offType, 1

    elif offCat == "drug-alcohol":
        return "drug-alcohol", offType, 1

    elif offCat == "auto-theft":
        return "theft", "auto-theft", 1

    elif offCat == "white-collar-crime":
        return "white-collar-crime", offType, 1

    elif offCat == "burglary":
        return "theft", offType, 1

    elif offCat == "public-disorder":
        return "mischief", offType, 1

    elif offCat == "aggravated-assault":
        return "off-against-person", offType, 2

    elif offCat == "other-crimes-against-persons":
        return "off-against-person", offType, 1

    elif offCat == "robbery":
        return "theft", offType, 1

    elif offCat == "sexual-assault":
        return "off-against-person", offType, 1

    elif offCat == "murder":
        return "homicide", offType, 2

    elif offCat == "arson":
        return "arson", offType, 1

    else:
        print("No match!!", offCat, offType)


def handle_denv(index):
    """ Process one row of the denver data
    """
    # Crime_report_time", "Crime_start_time", "Crime_end_time", "Crime_type", "Crime_category", "Crime_severity_index"
    row = GLOB_DENV_DATA.iloc[index]
    new_row = []

    # Crime_start_time
    first_occur = denv_parse_date(row["FIRST_OCCURRENCE_DATE"])
    if first_occur is not None:
        new_row.append(first_occur.strftime("%H:%M"))
    else:
        new_row.append("None")

    # Crime_report_time
    report_date = denv_parse_date(row["REPORTED_DATE"])
    if report_date is not None:
        new_row.append(report_date.strftime("%H:%M"))
    else:
        new_row.append("None")

    # Crime_end_time
    end_occur = denv_parse_date(row["LAST_OCCURRENCE_DATE"])
    if end_occur is not None:
        new_row.append(end_occur.strftime("%H:%M"))
    else:
        new_row.append("None")

    crime_cat, crime_type, crim_sev = denv_map_offense(row["OFFENSE_CATEGORY_ID"], row["OFFENSE_TYPE_ID"])

    # Crime_type
    new_row.append(crime_type)

    # Crime_category
    new_row.append(crime_cat)

    # Crime_severity_index
    new_row.append(crim_sev)

    return new_row


def handle_van(index):
    """ Process one row of the vancouver data
    """
    # Crime_report_time", "Crime_start_time", "Crime_end_time", "Crime_type", "Crime_category", "Crime_severity_index"
    row = GLOB_VAN_DATA.iloc[index]
    new_row = []

    # Crime_report_time
    new_row.append("None")

    # Crime_start_time
    start_time = datetime.datetime(row["YEAR"], row["MONTH"], row["DAY"], row["HOUR"], row["MINUTE"])
    new_row.append(start_time.strftime("%H:%M"))

    # Crime_end_time
    new_row.append("None")

    # Insert remaining data
    crime_cat, crime_type, crim_sev = van_map_offense(row["TYPE"])

    # Crime_type
    new_row.append(crime_type)

    # Crime_category
    new_row.append(crime_cat)

    # Crime_severity_index
    new_row.append(crim_sev)

    return new_row
