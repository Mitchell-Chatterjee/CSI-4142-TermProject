import pandas
denvData= pandas.read_csv("../data/denverCrime.csv")
vanData= pandas.read_csv("../data/vancouverCrime.csv")

def exampleCall(row):
    print(row)
    print("------------------")
    print(row["GEO_X"])

for index, row in denvData.iterrows():
    exampleCall(row)

for index, row in vanData.iterrows():
    exampleCall(row)