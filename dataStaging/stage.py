import pandas
import os.path
from connect import Database
import os

# file imports
import stageDate
import stageEvent

denvData= pandas.read_csv("data/denverCrime.csv")
vanData= pandas.read_csv("data/vancouverCrime.csv")

def exampleCall(row):
    print(row)
    print("------------------")
    print(row["GEO_X"])

# for index, row in denvData.iterrows():
#     exampleCall(row)

# for index, row in vanData.iterrows():
#     exampleCall(row)

def historicLoad():
    # event dimension
    if(os.path.isfile('data/transformed_data/transformed_date_data.csv')):
        print("Reading transformed date data")
        # TODO
        print("Finished reading transformed date data")
    else:
        print("Extracting transformed date data")
        date_data_transformed = stageDate.transform_date(vanData, True)
        print(date_data_transformed.head())
        print("Done extracting date data. Now enriching.")
        date_data_transformed = stageDate.enrich_date(date_data_transformed.head(), True)

    # date dimension
    if(os.path.isfile('data/transformed_data/transformed_event_data.csv')):
        print("Reading transformed event data")
        # TODO
        print("Finished reading transformed event data")
    else:
        print("Extracting transformed event data")
        date_data_transformed = stageDate.transform_date(vanData, True)
        print(date_data_transformed.head())

    # how to save file for next time
    #collision_data.to_csv('data/collisions/ottawa/collision_data_transformed.csv')


# main
dbConn = Database()
# This function will load the location data into the db.
def loadLocation():
    if os.path.isfile('../data/final/denverLocation.csv'):
        cursor = dbConn.cursor()
        cursor.execute('SET search_path="CSI4142"')
        print("Reading Transformed Denver location Data...")
        with open('../data/final/denverLocation.csv', 'r') as f:
            next(f)
            cursor.copy_from(f,'location', sep=',')
            dbConn.commit()
    else:
        print(" Creating 'denverLocation.csv' file and populating the file (This will take a while)")
        createLocationCsv("denver")
    print("DONE populating the Location table with the Denver data!")

    # Populating the location table with the Vancouver data.
    if os.path.isfile('../data/final/vancouverLocation.csv'):
        print("Reading Transformed Vancouver location Data...")
        with open('../data/final/vancouverLocation.csv', 'r') as f:
            next(f)
            cursor.copy_from(f,'location', sep=',')
            dbConn.commit()
    else:
        print(" Creating 'vancouverLocation.csv' file and populating the file (This will take a while)")
        createLocationCsv("vancouver")
    print("DONE populating the Location table with the Vancouver data!")

del dbConn

