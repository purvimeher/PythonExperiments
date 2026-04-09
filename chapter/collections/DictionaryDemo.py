from operator import truediv

from selenium.webdriver.common.utils import keys_to_typing

personDetails = {
    'name': 'Meher',
    'age': 45,
    'sex': 'Male',
    'skills': ['Java', 'CSharp', 'Python', 'Typescript', 'Javascript', 'SQL', 'CI/CD', 'Jenkins'],
    'employed': True,
    'projects': {
        'project_one': {
            'project_name': 'LEDSP',
            'company': 'HomeOffice'
        }, 'project_two': {
            'project_name': 'ITSD',
            'company': 'HMRC'
        }
    }
}

print(personDetails)

# print specific key
print(personDetails['skills'])

# print specific key in safe way
print(personDetails.get('employed'))
print(personDetails.get('name'))

# iterate through all keys in dict
for key, value in personDetails.items():
    print(key, value)

# using lambda work on keys in dict
newDefinitionOfPerson = {key.upper(): value for (key, value) in personDetails.items()}
print('New definitions of a Person ::')
print(newDefinitionOfPerson)

# print message based on filter
age_related_message = 'Middle aged employee' if personDetails.get('age') >= 45 else ' Younger employee'
print(age_related_message)

Company = [
    {
        'name': 'Meher',
        'age': 45,
        'sex': 'Male',
        'skills': ['Java', 'CSharp', 'Python', 'Typescript', 'Javascript', 'SQL', 'CI/CD', 'Jenkins'],
        'employed': True,
        'role': 'Test Analyst',
        'projects': {
            'project_one': {
                'project_name': 'LEDSP',
                'company': 'HomeOffice'
            }, 'project_two': {
                'project_name': 'ITSD',
                'company': 'HMRC'
            }
        }
    }, {
        'name': 'Sushmita',
        'age': 45,
        'sex': 'Female',
        'skills': ['Acounting'],
        'employed': True,
        'role': 'Finance Controller',
        'projects': {
            'project_one': {
                'project_name': 'LEDSP',
                'company': 'HomeOffice'
            }, 'project_two': {
                'project_name': 'ITSD',
                'company': 'HMRC'
            }
        }
    }, {
        'name': 'Hema',
        'age': 45,
        'sex': 'Female',
        'skills': ['Java', 'Typescript', 'Javascript'],
        'employed': True,
        'role': 'Test Analyst',
        'projects': {
            'project_one': {
                'project_name': 'LEDSP',
                'company': 'HomeOffice'
            }
        }
    }, {
        'name': 'Purvi',
        'age': 45,
        'sex': 'Female',
        'skills': ['Java'],
        'employed': True,
        'role': 'Test Analyst',
        'projects': {
            'project_two': {
                'project_name': 'ITSD',
                'company': 'HMRC'
            }
        }
    }
]

# print all employees within a company
for person in Company:
    message = f'Hello {person.get('name')} from department {person.get('role')}'
    print(message)


def my_filtering_function(pair):
    key, value = pair
    if key['sex'] == 'Female':
        return True  # keep pair in the filtered dictionary
    else:
        return False  # filter pair out of the dictionary
# print only female employee details
filtered_female_employees = [key for key in Company if key['sex'] == 'Female']
print(filtered_female_employees)
for employee in filtered_female_employees:
    print(f'{employee.get('name')} : {employee.get("role")}')