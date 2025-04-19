import os

if __name__ == '__main__':
    '''
    To start your new project, simply run this handy file
    What will it do:
    - Clear README.md
    - Change project name on pyproject.toml
    - Delete itself
    '''

    name_project = os. getcwd().rsplit('\\')[-1]

    print(f'Hello project {name_project}!')

    readme = 'README.md'
    with open(readme, 'w') as file:
        file.write(f'# {name_project}')

    toml = 'pyproject.toml'
    with open(toml, 'r+') as file:
        temp = file.read()
        temp = temp.replace('Template', name_project)
        file.seek(0)
        file.write(temp)
        file.truncate()

    os.remove('setup.py')