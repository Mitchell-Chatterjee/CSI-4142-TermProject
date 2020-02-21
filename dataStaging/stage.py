import pandas
import os.path
from connect import Database
import os

# file imports
import stageDate
import stageEvent
from location import createLocationCsv
from pop_crime import gen_crime_csvs

denvData= pandas.read_csv("../data/filteredDenverCrime.csv")
vanData= pandas.read_csv("../data/filteredVancouverCrime.csv")

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
        event_data_transformed = pandas.read_csv('../data/transformed_data/transformed_event_data.csv', parse_dates = ['Event_date'])
        print("Finished reading transformed event data")
    else:
        print("Transforming and extracting event data")
        event_data_transformed = stageEvent.transform_event_data()
        # write event data to csv for future
        event_data_transformed.to_csv('../data/transformed_data/transformed_event_data.csv', index=False)

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

    # crime dimension
    if os.path.isfile('../data/final/denvCrimeDim.csv') \
            and os.path.isfile('../data/final/vanCrimeDim.csv'):
        print("Crime dimension csv files found")
    else:
        print("No Crime csv files found. Rebuilding crime files")
        gen_crime_csvs(vanData, denvData)

    #loadCrime()

    # how to save file for next time
    # collision_data.to_csv('data/collisions/ottawa/collision_data_transformed.csv')


# main
dbConn = Database()
# This function will load the location data into the db.
def loadLocation(city):
    if(city == "denver"):
        if os.path.isfile('../data/final/denverLocation.csv'):
            dbConn.cursor.execute('SET search_path="CSI4142"')
            print("Reading Transformed Denver location Data...")
            with open('../data/final/denverLocation.csv', 'r') as f:
                next(f)
                dbConn.cursor.copy_from(f,'location', sep=',')
                dbConn.connection.commit()
        else:
            print(" Creating 'denverLocation.csv' file and populating the file (This will take a while)")
            createLocationCsv("denver")
            loadLocation("denver")
        print("DONE populating the Location table with the Denver data!")
    else:
        # Populating the location table with the Vancouver data.
        if os.path.isfile('../data/final/vancouverLocation.csv'):
            dbConn.cursor.execute('SET search_path="CSI4142"')
            print("Reading Transformed Vancouver location Data...")
            with open('../data/final/vancouverLocation.csv', 'r') as f:
                next(f)
                dbConn.cursor.copy_from(f,'location', sep=',')
                dbConn.connection.commit()
        else:
            print(" Creating 'vancouverLocation.csv' file and populating the file (This will take a while)")
            createLocationCsv("vancouver")
            loadLocation("vancouver")
        print("DONE populating the Location table with the Vancouver data!")

# This function will load the crime data into the db.
def loadCrime():
    if os.path.isfile('../data/final/denvCrimeDim.csv') \
            and os.path.isfile('../data/final/vanCrimeDim.csv'):
        dbConn.cursor.execute('SET search_path="CSI4142"')

        print("Reading Transformed Denver crim Data...")
        with open('../data/final/denvCrimeDim.csv', 'r') as f:
            next(f)
            dbConn.cursor.copy_from(f,'crime', sep=',', null="None")
            dbConn.connection.commit()
        print("DONE populating the crime table with the Denver data!")

        print("Reading Transformed Vancouver crime Data...")
        with open('../data/final/vanCrimeDim.csv', 'r') as f:
            next(f)
            dbConn.cursor.copy_from(f,'crime', sep=',', null="None")
            dbConn.connection.commit()
        print("DONE populating the crime table with the vancouver data!")
    else:
        print("Error fact csvs do not exist")

historicLoad()

del dbConn
