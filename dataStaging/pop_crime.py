import datetime
from math import isnan
import pandas
from multiprocessing import Process

""" Handles the population of the Crime table

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


def gen_crime_csvs(vanData, denvData):
    """ Creates two csvs, one for denver and one for vancouver 
    This function should be run prior to loadCrime if the CSVs
    do not already exist

    !!!! If changes to the length of data is made, the hardcoded count
    value in gen_van_csv MUST be changed !!!!!
    """
    denv_proc = Process(target=gen_denv_csv, args=(denvData,))
    denv_proc.start()

    van_proc = Process(target=gen_van_csv, args=(vanData,))
    van_proc.start()

    denv_proc.join()
    van_proc.join()


def gen_denv_csv(denvData):
    denvDf = pandas.DataFrame(columns=["Crime_key", "Crime_report_time", "Crime_start_time", "Crime_end_time", "Crime_type", "Crime_category", "Crime_severity_index"])

    print("Crime dimension: Processing denver data...")
    count = 1
    for i, row in denvData.iterrows():
        new_row = handle_denv(row, count)
        count = count + 1
        if new_row is not None:
            denvDf.loc[len(denvDf)] = new_row
    print("Crime dimension: Done denver")

    print("Crime dimension: Writing denver to csv (denvCrimeDim.csv)...")
    denvDf.to_csv("../data/final/denvCrimeDim.csv", encoding='utf-8', index=False)
    print("Crime dimension:  Denver write complete")


def gen_van_csv(vanData):
    vanDf = pandas.DataFrame(columns=["Crime_key", "Crime_report_time", "Crime_start_time", "Crime_end_time", "Crime_type", "Crime_category", "Crime_severity_index"])
   
    # HARDCODED TODO
    count = 277866 + 1 
    print("Crime dimension: Processing vancouver data...")
    for i, row in vanData.iterrows():
        new_row = handle_van(row, count)
        count = count + 1
        if new_row is not None:
            vanDf.loc[len(vanDf)] = new_row
    print("Crime dimension: Done vancouver")

    print("Crime dimension: Writing vancouver to csv (vanCrimDim.csv)...")
    vanDf.to_csv("../data/final/vanCrimeDim.csv", encoding='utf-8', index=False)
    print("Crime dimension: Vancouver write complete")
 

def denv_parseDate(inDate):
    """ Process a date string from denver data and return corresponding datetime
    """
    if isinstance(inDate, str):
        date = datetime.datetime.strptime(inDate, "%m/%d/%Y %I:%M:%S %p")
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


def handle_denv(row, key):
    """ Process one row of the denver data
    """
    # Crime_key Crime_report_time", "Crime_start_time", "Crime_end_time", "Crime_type", "Crime_category", "Crime_severity_index"
    new_row = []
    new_row.append(key)

    # Crime_start_time
    first_occur = denv_parseDate(row["FIRST_OCCURRENCE_DATE"])
    if first_occur is not None:
        new_row.append(first_occur.strftime("%H:%M"))
    else:
        new_row.append("None")

    # Crime_report_time
    report_date = denv_parseDate(row["REPORTED_DATE"])
    if report_date is not None:
        new_row.append(report_date.strftime("%H:%M"))
    else:
        new_row.append("None")

    # Crime_end_time
    end_occur = denv_parseDate(row["LAST_OCCURRENCE_DATE"])
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


def handle_van(row, key):
    """ Process one row of the vancouver data
    """
    # Crime_key Crime_report_time", "Crime_start_time", "Crime_end_time", "Crime_type", "Crime_category", "Crime_severity_index"
    new_row = []
    new_row.append(key)

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
