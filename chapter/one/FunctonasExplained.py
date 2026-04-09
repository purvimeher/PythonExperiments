import random


def firstFunc(greetings=["Hello", "Hi","Hola"], name ="Buddy!"):
    print(random.choice(greetings) + " " + name.capitalize())

def secondFunc(greetings, name ="Buddy!"):
    print(greetings + " " + name.capitalize())

def thirdFunc(greetings, name):
    print(greetings + " " + name.capitalize())


def greetAccordingToAge(age):
    if age < 18:
        print("Hi Buddy!")
    elif age < 45:
        print("Hello Buddy!")
    else:
        print("Namaskar Buddy!")


def recurssiveFunc(count):
    if count > 0:
        print(f'count + {count}')
        count -= 1
        recurssiveFunc(count)


# without optional parameters
firstFunc()
secondFunc("Namaskar", "Meher!")
secondFunc("Hi")
thirdFunc("Hi", "avatar meher baba ki jai!")
greetAccordingToAge(10)
greetAccordingToAge(40)
greetAccordingToAge(60)
recurssiveFunc(5)