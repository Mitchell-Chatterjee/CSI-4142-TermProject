#%%
import csv
import psycopg2
from database import Database
import pandas as pd
import os.path
from os import path
from numpy import genfromtxt, savetxt, count_nonzero

# Returns the address of the entry
def locationName(row, city):
    if city == "denver":
        if pd.isna(row['INCIDENT_ADDRESS']):
            return "UNKNOWN"
        else:
            return row['INCIDENT_ADDRESS']
    else:
        return row['HUNDRED_BLOCK']

# Returns the X coodinate of the entry
def locationX(row, city):
    if city == "denver":
        if pd.isna(row['GEO_X']):
            return -1
        else:
            return row['GEO_X']
    else:
        return row['X']

# Returns the Y coodinate of the entry
def locationY(row, city):
    if city == "denver":
        if pd.isna(row['GEO_Y']):
            return -1
        else:
            return row['GEO_Y']
    else:
        return row['Y']

# Returns the Neighbourhood name of the entry
def locationNeighbourhood(row, city):
    if city == "denver":
        return row['NEIGHBORHOOD_ID']
    else:
        return row['NEIGHBOURHOOD']

# Returns the City of the entry
def locationCity(row, city):
    if city == "denver":
        return "Denver"
    else:
        return "Vancouver"

# Returns the crimerate of the entry
def locationCrimerate(row, city):
    if city == "denver":
        denverNumberOfCrime = 1
        denverPopulation = 1

        denverCrime = csv.reader(open('data/denverCrimeRate2018.csv', "rt",encoding="utf8"), delimiter=",")
        for x in denverCrime:
            if x[0] == row['NEIGHBORHOOD_ID']:
                denverNumberOfCrime=x[14]

        denverInfo = csv.reader(open('data/denverInfo.csv', "rt",encoding="utf8"), delimiter=",")
        for x in denverInfo:
            if x[0] == row['NEIGHBORHOOD_ID']:
                denverPopulation = x[1]

        return (int(denverNumberOfCrime)/int(denverPopulation))*100000
    else:
        vancouverNumberOfCrime = 1
        vancouverPopulation = 1

        vancouverCrime = csv.reader(open('data/vancouverCrimeRate2018.csv', "rt",encoding="utf8"), delimiter=",")
        for x in vancouverCrime:
            if x[0] == row['NEIGHBOURHOOD']:
                vancouverNumberOfCrime=x[11]

        vancouverInfo = csv.reader(open('data/vancouverInfo.csv', "rt",encoding="utf8"), delimiter=",")

        for x in vancouverInfo:
            if x[0] == row['NEIGHBOURHOOD']:
                vancouverPopulation = x[3]

        return (int(vancouverNumberOfCrime)/int(vancouverPopulation))*100000

# Returns the number of crimes of the entry
def locationNumberOfCrimes(row, city):
    if city == "denver":
        denverNumberOfCrime = 1

        denverCrime = csv.reader(open('data/denverCrimeRate2018.csv', "rt",encoding="utf8"), delimiter=",")
        for x in denverCrime:
            if x[0] == row['NEIGHBORHOOD_ID']:
                denverNumberOfCrime=x[14]

        return denverNumberOfCrime
    else:
        vancouverNumberOfCrime = 1

        vancouverCrime = csv.reader(open('data/vancouverCrimeRate2018.csv', "rt",encoding="utf8"), delimiter=",")
        for x in vancouverCrime:
            if x[0] == row['NEIGHBOURHOOD']:
                vancouverNumberOfCrime=x[11]

        return vancouverNumberOfCrime

# Returns the Avg house hold income of the entry
def locationAvgHouseholdIncome(row, city):
    if city == "denver":
        denverHouseholdIncome = 1

        denverInfo = csv.reader(open('data/denverInfo.csv', "rt",encoding="utf8"), delimiter=",")
        for x in denverInfo:
            if x[0] == row['NEIGHBORHOOD_ID']:
                denverHouseholdIncome=x[4]

        return denverHouseholdIncome
    else:
        vancouverHouseholdIncome = 1

        vancouverInfo = csv.reader(open('data/vancouverInfo.csv', "rt",encoding="utf8"), delimiter=",")
        for x in vancouverInfo:
            if x[0] == row['NEIGHBOURHOOD']:
                vancouverHouseholdIncome=x[7]

        return vancouverHouseholdIncome

# Returns the Avg Property value of the entry
def locationAvgPropValue(row, city):
    if city == "denver":
        denverAvgPropValue = 1

        denverInfo = csv.reader(open('data/denverInfo.csv', "rt",encoding="utf8"), delimiter=",")
        for x in denverInfo:
            if x[0] == row['NEIGHBORHOOD_ID']:
                denverAvgPropValue=x[3]

        return denverAvgPropValue
    else:
        vancouverAvgPropValue = 1

        vancouverInfo = csv.reader(open('data/vancouverInfo.csv', "rt",encoding="utf8"), delimiter=",")
        for x in vancouverInfo:
            if x[0] == row['NEIGHBOURHOOD']:
                vancouverAvgPropValue=x[6]

        return vancouverAvgPropValue

# Returns the number of precincts of the entry
def locationNumOfPrecincts(row, city):
    if city == "denver":
        denverNumPrecincts = 0

        denverInfo = csv.reader(open('data/denverInfo.csv', "rt",encoding="utf8"), delimiter=",")
        for x in denverInfo:
            if x[0] == row['NEIGHBORHOOD_ID']:
                denverNumPrecincts=x[5]

        return denverNumPrecincts
    else:
        vancouverNumPrecincts = 0

        vancouverInfo = csv.reader(open('data/vancouverInfo.csv', "rt",encoding="utf8"), delimiter=",")
        for x in vancouverInfo:
            if x[0] == row['NEIGHBOURHOOD']:
                vancouverNumPrecincts=x[8]

        return vancouverNumPrecincts

# Returns a list of all the columns(attributes) of a row
def generateColumn(row,counter,city):
    columns =[]
    columns.append(counter) 
    columns.append(locationName(row, city))
    columns.append(locationX(row, city))
    columns.append(locationY(row, city))
    columns.append(locationNeighbourhood(row, city))
    columns.append(locationCity(row, city))
    columns.append(locationCrimerate(row, city))
    columns.append(locationNumberOfCrimes(row, city))
    columns.append(locationAvgHouseholdIncome(row, city))
    columns.append(locationAvgPropValue(row, city))
    columns.append(locationNumOfPrecincts(row, city))
    return columns

# This function creates the csv files for both cities to populate the database.
# Before calling this function make sure the denverLocation.csv and vancouverLocation.csv files are empty
def createLocationCsv():
    # ------------------------------------------------Denver csv file populating-----------------------------------------------------------
    denvor_dataset = pd.read_csv('data/denverCrime.csv')

    locationRowsDenver = []
    counter = 1

    numberOfRowsDenver=len(denvor_dataset.axes[0])

    for x in range(numberOfRowsDenver):
        denverRow = generateColumn(denvor_dataset.iloc[x],counter, "denver")
        locationRowsDenver.append(denverRow)
        counter = counter + 1

    if(path.exists("data/final/denverLocation.csv") != False):
        with open('data/final/denverLocation.csv','w', newline= '') as f:
            writer = csv.writer(f)
            writer.writerow(['Location_key','Location_name','GeoX','GeoY','Neighbourhood','City','Crime_rate','NumberOfCrimes','AvgHouseholdIncome','AvgPropValue','NumOfPrecincts'])
            for row in locationRowsDenver:
                writer.writerow(row)
    else:
        print("!! The denverLocation.csv file doesn't exist mate, call me back when you create it !!")
        return
# ------------------------------------------------Vancouver csv file populating-----------------------------------------------------------
    vancouver_dataset = pd.read_csv('data/vancouverCrime.csv')
    
    locationRowsVancouver = []
    numberOfRowsVancouver=len(vancouver_dataset.axes[0])

    for x in range(numberOfRowsVancouver):
        vancouverRow = generateColumn(vancouver_dataset.iloc[x],counter, "vancouver")
        locationRowsVancouver.append(vancouverRow)
        counter = counter + 1

    if(path.exists("data/final/vancouverLocation.csv") != False):
        with open('data/final/vancouverLocation.csv','w', newline= '') as f:
            writer = csv.writer(f)
            writer.writerow(['Location_key','Location_name','GeoX','GeoY','Neighbourhood','City','Crime_rate','NumberOfCrimes','AvgHouseholdIncome','AvgPropValue','NumOfPrecincts'])
            for row in locationRowsVancouver:
                writer.writerow(row)
    else:
        print("!! The vancouverLocation.csv file doesn't exist mate, call me back when you create it !!")
        return

# function to populated the Database with the final values of all attributes.
# Keep in mind dever should be added before Vancouver due to the primary key if you decide
# to comment code out.
# This function pushes the csv files created from the createLocationCsv to the db.
# Before calling the function add yur username and password in the connection row
def populateDatabase(city):
    if(city == "denver"):
        # Populating the location table with the Denver data.
        conn = psycopg2.connect(user = "dalga082",password = "Sheyanthi.1997",host = "www.eecs.uottawa.ca",port = "15432",database = "group_2") 
        cursor = conn.cursor()
        cursor.execute('SET search_path="CSI4142"')
        with open('data/final/denverLocation.csv', 'r') as f:
            next(f)
            cursor.copy_from(f,'location', sep=',')
            conn.commit()
        conn.close()
        print("DONE populating the Location table with the Denver data.")
    else:
        # Populating the location table with the Vancouver data.
        conn = psycopg2.connect(user = "dalga082",password = "Sheyanthi.1997",host = "www.eecs.uottawa.ca",port = "15432",database = "group_2") 
        cursor = conn.cursor()
        cursor.execute('SET search_path="CSI4142"')
        with open('data/final/vancouverLocation.csv', 'r') as f:
            next(f)
            cursor.copy_from(f,'location', sep=',')
            conn.commit()
        conn.close()
        print("DONE populating the Location table with the Vancouver data.")

# Creates a list of all the primary keys in the location table.
def locationPrimaryKeys():
    denver_count=0
    vancouver_count=0
    with open('data/final/denverLocation.csv') as f:
        denver_count = sum(1 for line in f)
    with open('data/final/vancouverLocation.csv') as f:
        vancouver_count = sum(1 for line in f)
    keys = []
    for x in range(denver_count+vancouver_count-2):
        if(x > 0):
            keys.append(x)
    return keys

if __name__ == '__main__':
    # createLocationCsv()
    # populateDatabase('denver')
    # populateDatabase('vancouver')