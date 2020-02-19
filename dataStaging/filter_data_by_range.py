from multiprocessing import Process
from csv import reader, writer, QUOTE_MINIMAL
import datetime


def main():
    denv_proc = Process(target=filter_denv)
    denv_proc.start()

    van_proc = Process(target=filter_van)
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

def filter_denv():
    print("Filter denv data...")
    upperBound = datetime.datetime(2018, 12, 31)
    lowerBound = datetime.datetime(2016, 1, 1)
    with open('filteredDenverCrime.csv', mode='w') as out_file:
        csvWriter = writer(out_file, delimiter=',', quotechar='"', quoting=QUOTE_MINIMAL)
        csvWriter.writerow("")
        with open("../data/denverCrime.csv") as in_file:
            csvReader = reader(in_file, delimiter=',')
            # copy header
            header = next(csvReader)
            csvWriter.writerow(header)
            # Filter rows
            for row in csvReader:
                # First_occurrence_date
                first_occur = denv_parseDate(row[6])
                if not (lowerBound <= first_occur <= upperBound):
                    continue
                else:
                    csvWriter.writerow(row)
    print("denv data done")


def filter_van():
    print("Filter van data...")

    with open('filteredVancouverCrime.csv', mode='w') as out_file:
        csvWriter = writer(out_file, delimiter=',', quotechar='"', quoting=QUOTE_MINIMAL)
        with open("../data/vancouverCrime.csv") as in_file:
            csvReader = reader(in_file, delimiter=',')
            # copy header
            header = next(csvReader)
            csvWriter.writerow(header)
            # Filter rows
            for row in csvReader:
                # Hour
                if not (2016 <= int(row[1]) <= 2018):
                    continue
                else:
                    csvWriter.writerow(row)
    print("van data done")


if __name__ == "__main__":
    main()
