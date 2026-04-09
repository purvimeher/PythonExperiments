def get_full_name(firstName, lastName):
    return "{} {}".format(firstName, lastName)



FullName = get_full_name("Meher", "Meka")
print(FullName)


def get_full_details_of_person(first_name, last_name, age, param):
    return "{} {} {}".format(first_name, last_name, age)


FullDetailsOfPerson =  get_full_details_of_person("Meher", "Meka", 45, lambda first_name, last_name, age: f"{last_name}, {first_name} {age}")
print(FullDetailsOfPerson)