class FirstClass:

    @staticmethod
    def sampleStaticMethod():
        print('From Static Method')

    def __init__(self, firstName):
        self.firstName = firstName
        print(self.firstName)

    def printFirstName(self):
        print(self.firstName)


firstClass = FirstClass('Meher')
firstClass.firstName = 'Saibaba'
print(firstClass.firstName)
firstClass.printFirstName()
FirstClass.sampleStaticMethod()