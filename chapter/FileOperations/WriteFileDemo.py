import os.path

lines =['Avatar Meher Baba Ki Jai','Sachitananda Paramanda Meher Baba Vigyananda', 'Om Sai Ram','Hari Ashtakam']

def WriteContentToExistingFile():
    with open('/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/FileOperations/SampleFile','a') as f:
        for line in lines:
            f.write(line)
            f.write('\n')


def WriteContentToNewFile():
    fileName : string = '/Users/mehermeka/PycharmProjects/PythonProjectSelenium/chapter/FileOperations/SampleFile_one'
    with open(fileName, 'w') as f:
        for line in lines:
            f.write(line)
            f.write('\n')
    # check if file exists or not
    fileExists : bool = os.path.exists(fileName)
    print(fileExists)


WriteContentToNewFile()