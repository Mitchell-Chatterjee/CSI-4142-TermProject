import pandas
import os.path
from connect import Database
from location import createLocationCsv

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

del dbConn
