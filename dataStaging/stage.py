import pandas
import os.path
from connect import Database
from location import createLocationCsv

denvData= pandas.read_csv("../data/denverCrime.csv")
vanData= pandas.read_csv("../data/vancouverCrime.csv")

def exampleCall(row):
    print(row)
    print("------------------")
    print(row["GEO_X"])

# for index, row in denvData.iterrows():
#     exampleCall(row)

# for index, row in vanData.iterrows():
#     exampleCall(row)


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

