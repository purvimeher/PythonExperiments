# When a function has the **kwargs parameter, it can accept a variable number of keyword arguments as a dictionary.

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

def printPersonDetails(**kwargs):
    print(kwargs)



printPersonDetails(**personDetails)

printPersonDetails(a=1,b=2,c=3)