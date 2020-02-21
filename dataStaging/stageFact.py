from multiprocessing import Process
from csv import writer, QUOTE_MINIMAL
import datetime
import pandas
from math import isnan

def gen_fact_csvs(vanData, denvData, date_data_transformed):
    """ Creates two CSVs containing the Fact Tables for each city

    !!!! If changes to the length of data is made, the hardcoded count
    value in gen_van_csv MUST be changed !!!!!
    """

    denv_proc = Process(target=gen_denv_csv, args=(denvData, date_data_transformed["Event_keys"]))
    denv_proc.start()

    van_proc = Process(target=gen_van_csv, args=(vanData, date_data_transformed["Event_keys"]))
    van_proc.start()

    denv_proc.join()
    van_proc.join()


def denv_parseDate(inDate):
    """ Process a date string from denver data and return corresponding datetime
    """
    if isinstance(inDate, str):
        date = datetime.datetime.strptime(inDate, "%m/%d/%Y %I:%M:%S %p")
    else:
        date = None
    return date

def gen_denv_csv(denvData, eventDateMap):
    header = ["Date_key", "Location_key", "Crime_key", "Event_key", "Is_traffic", "Is_fatal", "Is_Nighttime"]

    print("Fact Table: Processing denver data")
    with open("../data/final/denvFact.csv", mode='w') as out_file:
        csvWriter = writer(out_file, delimiter=',', quotechar='"', quoting=QUOTE_MINIMAL)
        csvWriter.writerow(header)
        count = 1
        for i, row in denvData.iterrows():
            # Iterate through all the events of today
            # Want to index eventDateMap starting at 0 thus count -1 
            # [1] is to access the list part of the tuple
            for eventKey in eventDateMap.iloc[count - 1][1]:

                # Handle event keys
                new_row = [count, count, count]

                if isnan(eventKey):  # No event today
                    new_row.append(0)
                else:  # Add the event key
                    new_row.append(eventKey)

                # Handle Is_traffic
                if row["OFFENSE_CATEGORY_ID"] == "traffic-accident":
                    new_row.append("true")
                else:
                    new_row.append("false")

                # Handle Is_fatal
                if row["OFFENSE_CATEGORY_ID"] == "murder":
                    new_row.append("true")
                else:
                    new_row.append("false")

                # Handle Is_Nighttime
                first_occur = denv_parseDate(row["FIRST_OCCURRENCE_DATE"])
                if first_occur.hour >= 20:  # Check if later then or is 8
                    new_row.append("true")
                else:
                    new_row.append("false")

                csvWriter.writerow(new_row)

            count = count + 1
    print("Fact Table: Done denver")


def gen_van_csv(vanData, eventDateMap):
    header = ["Date_key", "Location_key", "Crime_key", "Event_key", "Is_traffic", "Is_fatal", "Is_Nighttime"]

    # !!!! HARDCODED: MUST MATCH THE VALUE OF THE LAST KEY OF DENVER DATA !!!!
    count = 277866 + 1
   
    print("Fact Table: Processing vancouver data...")
    with open("../data/final/vanFact.csv", mode='w') as out_file:
        csvWriter = writer(out_file, delimiter=',', quotechar='"', quoting=QUOTE_MINIMAL)
        csvWriter.writerow(header)
        for i, row in vanData.iterrows():
            # Iterate through all the events of today
            # Want to index eventDateMap starting at 277865 thus count - 2
            # [1] is to access the list part of the tuple
            for eventKey in eventDateMap.iloc[count - 2][1]:

                # Handle event keys
                new_row = [count, count, count]

                if isnan(eventKey):  # No event today
                    new_row.append(0)
                else:  # Add the event key
                    new_row.append(eventKey)

                rowType = row["TYPE"]

                # Handle Is_traffic
                if rowType == "Vehicle Collision or Pedestrian Struck (with Injury)" \
                        or rowType == "Vehicle Collision or Pedestrian Struck (with Fatality)":
                    new_row.append("true")
                else:
                    new_row.append("false")

                # Handle Is_fatal
                if rowType == "Homicide":
                    new_row.append("true")
                else:
                    new_row.append("false")

                # Handle Is_Nighttime
                if row["HOUR"] >= 20:  # Check if later then or is 8
                    new_row.append("true")
                else:
                    new_row.append("false")
                csvWriter.writerow(new_row)

            count = count + 1
    print("Fact Table: Done vancouver")

