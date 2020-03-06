import pandas
import os.path
from connect import Database
import os
from location import createLocationCsv
from stageFact import gen_fact_csvs

# file imports
import stageDate
import stageEvent
from location import createLocationCsv
from pop_crime import gen_crime_csvs

denvData= pandas.read_csv("../data/filteredDenverCrime.csv")
vanData= pandas.read_csv("../data/filteredVancouverCrime.csv")


def historicLoad(dbConn):
    loadLocation("denver")
    loadLocation("vancouver")
    # event dimension
    event_data_transformed = []
    if(os.path.isfile('../data/transformed_data/transformed_event_data.csv')):
        print("Reading transformed event data")
        event_data_transformed = pandas.read_csv('../data/transformed_data/transformed_event_data.csv', parse_dates = ['Event_date'])
        print("Finished reading transformed event data")
    else:
        print("Transforming and extracting event data")
        event_data_transformed = stageEvent.transform_event_data()
        # write event data to csv for future
        event_data_transformed.to_csv('../data/transformed_data/transformed_event_data.csv', index=False)
        print("Done extracting event data")
        print()
    # load event data to the database
    stageEvent.load_to_database(event_data_transformed, dbConn)
    


    # date dimension
    print("Extracting transformed date data")
    date_data_transformed = stageDate.transform_date({'Denver':denvData, 'Vancouver':vanData}, event_data_transformed, dbConn)
    print("Enriching date data")


    # crime dimension
    if os.path.isfile('../data/final/crimeDim.csv'):
        print("Crime dimension csv files found")
    else:
        print("Crime csv file not found. Rebuilding crime dimension...")
        gen_crime_csvs(vanData, denvData)

    loadCrime()

    # fact dimension
    if os.path.isfile('../data/final/denvFact.csv') and os.path.isfile('../data/final/vanFact.csv'):
        print("Fact csv files found")
    else:
        print("No fact csv files found. Rebuilding fact files")
        gen_fact_csvs(vanData, denvData, date_data_transformed)
    
    # !!! This part must be at the end !!!
    loadFact()


# This function will load the location data into the db.
def loadLocation(city):
    if(city == "denver"):
        if os.path.isfile('../data/final/denverLocation.csv'):
            try:
                print("Reading Transformed Denver location Data...")
                with open('../data/final/denverLocation.csv', 'r') as f:
                    next(f)
                    dbConn.writeFiletoDB(f, 'location')
            except Exception as err:
                print("---------------")
                print("Exception: ", err)
                print("---------------")
        else:
            print(" Creating 'denverLocation.csv' file and populating the file (This will take a while)")
            createLocationCsv("denver")
            loadLocation("denver")
    else:
        # Populating the location table with the Vancouver data.
        if os.path.isfile('../data/final/vancouverLocation.csv'):
            try:
                print("Reading Transformed Vancouver location Data...")
                with open('../data/final/vancouverLocation.csv', 'r') as f:
                    next(f)
                    dbConn.writeFiletoDB(f, 'location')
            except Exception as err:
                print("---------------")
                print("Exception: ", err)
                print("---------------")


        else:
            print(" Creating 'vancouverLocation.csv' file and populating the file (This will take a while)")
            createLocationCsv("vancouver")
            loadLocation("vancouver")

# This function will load the crime data into the db.
def loadCrime():
    if os.path.isfile('../data/final/denvCrimeDim.csv') \
            and os.path.isfile('../data/final/vanCrimeDim.csv'):
        try:
            print("Reading Transformed Denver crim Data...")
            with open('../data/final/denvCrimeDim.csv', 'r') as f:
                next(f)
                dbConn.writeFiletoDB(f, 'crime')

            print("Reading Transformed Vancouver crime Data...")
            with open('../data/final/vanCrimeDim.csv', 'r') as f:
                next(f)
                dbConn.writeFiletoDB(f, 'crime')
        except Exception as err:
            print("---------------")
            print("Exception: ", err)
            print("---------------")
    else:
        print("Error fact csvs do not exist")

# This function will load the fact data into the db.
def loadFact():
    if os.path.isfile('../data/final/denvFact.csv') and os.path.isfile('../data/final/vanFact.csv'):
        try:
            print("Reading Denver fact Data...")
            with open('../data/final/denvFact.csv', 'r') as f:
                next(f)
                dbConn.writeFiletoDB(f, 'crime_fact')

            print("Reading Vancouver fact Data...")
            with open('../data/final/vanFact.csv', 'r') as f:
                next(f)
                dbConn.writeFiletoDB(f, 'crime_fact')
        except Exception as err:
            print("---------------")
            print("Exception: ", err)
            print("---------------")
    else:
        print("Error: fact csvs do not exist")


# main
dbConn = Database()
# load location data
# load all other data
historicLoad(dbConn)
del dbConn
