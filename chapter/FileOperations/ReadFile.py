def ReadSampleFile():
    with open('/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/FileOperations/SampleFile') as f:
        lines = f.readlines()
        print(lines)

# A more concise way to read a text file line by line

def ReadFileLineByLine():
    with open('/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/FileOperations/SampleFile') as f:
        for line in f:
            print(line.strip())


# ReadSampleFile()
ReadFileLineByLine()