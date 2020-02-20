import pandas
import os.path
from connect import Database
import os

# file imports
import stageDate
import stageEvent

denvData= pandas.read_csv("data/filteredDenverCrime.csv")
vanData= pandas.read_csv("data/filteredVancouverCrime.csv")

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
    event_data_transformed = []
    if(os.path.isfile('data/transformed_data/transformed_event_data.csv')):
        print("Reading transformed event data")
        event_data_transformed = pandas.read_csv('data/transformed_data/transformed_event_data.csv', parse_dates = ['Event_date'])
        print("Finished reading transformed event data")
    else:
        print("Transforming and extracting event data")
        event_data_transformed = stageEvent.transform_event_data()
        # write event data to csv for future
        event_data_transformed.to_csv('data/transformed_data/transformed_event_data.csv', index=False)

    # need to merge event data with date data


    # date dimension
    date_data_transformed = pandas.DataFrame()
    if(os.path.isfile('data/transformed_data/enriched_date_data.csv')):
        print("Reading enriched date data")
        date_data_transformed = pandas.read_csv('data/transformed_data/enriched_date_data.csv')
        print("Finished reading enriched date data")
    else:
        print("Extracting transformed date data")
        date_data_transformed = stageDate.transform_date({'Denver':denvData, 'Vancouver':vanData}, event_data_transformed)
        print(date_data_transformed.head())
        print("Enriching date data")

    # how to save file for next time
    # collision_data.to_csv('data/collisions/ottawa/collision_data_transformed.csv')

historicLoad()

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

