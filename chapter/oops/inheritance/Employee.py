class Person:
    counter = 0

    def __init__(self, name, age):
        self.name = name
        self.age = age
        Person.counter += 1

    def greet(self):
        return f"Hi, I am from person, My name is {self.name}."

    @classmethod
    def create_anonymous(cls):
        return Person('Anonymous', 22)


class Employee(Person):
    def __init__(self, name, age, job_title):
        super().__init__(name, age)
        self.job_title = job_title

    def greet(self):
        return f" I'm an employee {self.job_title}."

employee = Employee('Meher', 45, 'Python Developer')
person = Person('GenericPerson', 45)
print(employee.greet())
print(issubclass(Employee, Person))
print(type(employee))
print(isinstance(employee, Person))
print(isinstance(employee, Employee))
print(isinstance(person, Person))
print(isinstance(person, Employee))

