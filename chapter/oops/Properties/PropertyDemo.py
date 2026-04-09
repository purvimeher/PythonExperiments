class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    # string representation of an object in human readable form
    def __str__(self):
        return f'My name is {self.name} and my age is {self.age}'

    # string representation of an object in machine readable form
    def __repr__(self):
        return f'My name is {self.name} and my age is {self.age}'

    # comparing two instances based on properties
    def __eq__(self, other):
        return self.name == other.name and self.age == other.age

    def __hash__(self):
        return hash(self.name)

    def __bool__(self):
        if(self.age == 0) or (self.age == 70):
            return False
        return True

meher = Person('Meher', 45)
aadhya = Person('Aadhya', 6)
unknown = Person('UnknownPerson', 0)
print(meher)
print(repr(meher))
print(meher.__eq__(aadhya))
print(hash(meher))
print(bool(meher))
print(bool(aadhya))
print(bool(unknown))
