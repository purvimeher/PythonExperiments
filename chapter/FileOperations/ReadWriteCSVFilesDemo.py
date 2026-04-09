import csv


# plain reading of csv file
def ReadCsvFile():
    csvFile = '/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/FileOperations/MOCK_DATA.csv'
    with open(csvFile) as f:
        csvReader = csv.reader(f)
        for row in csvReader:
            print(row)


def ReadingCsvFileIntoADictionary():
    csvFile = '/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/FileOperations/MOCK_DATA.csv'
    with open(csvFile) as f:
        csvReader = csv.DictReader(f)
        for row in csvReader:
            print(row)


def WriteintoCSVFileUsingaDictionary():
    sampleData = {'id': '11', 'first_name': 'Meher', 'last_name': 'Meka',
                  'email': 'mehermeka@gmail.com', 'gender': 'Male', 'ip_address': '127.0.0.1'}
    headers = sampleData.keys()
    csvFile = '/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/FileOperations/MOCK_DATA.csv'
    with open(csvFile,'a') as f:
        csvWriter = csv.DictWriter(f,  fieldnames=headers)
        csvWriter.writeheader()
        csvWriter.writerow(sampleData)

# ReadCsvFile()
WriteintoCSVFileUsingaDictionary()
ReadingCsvFileIntoADictionary()
