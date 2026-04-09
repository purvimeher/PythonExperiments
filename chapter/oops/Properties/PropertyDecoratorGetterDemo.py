class Person:
    def __init__(self, name, age):
        self.name = name
        self._age = age

    def get_age(self):
        return self._age

    age = property(fget=get_age)


meher = Person('Meher', 18)
print(meher.get_age())
print(meher.__dict__)