class Person:
    def __init__(self, name, age):
        self.name = name
        self.set_age(age)

    def set_age(self, age):
        if age <= 0:
            raise ValueError('The age must be positive')
        self._age = age

    def get_age(self):
        return self._age

    age = property(get_age, set_age)

john = Person('John', 18)
# john.set_age(-19)

john.set_age(20)
print(john.__dict__)