import setuptools
from setuptools import find_packages


with open("./requirements/requirements.in", "r") as req_file:
    REQUIREMENTS = req_file.read().splitlines()

REQUIREMENTS.append("helper_functions_ea")

setuptools.setup(
    name="Add an appropriate package name here!!",
    version='0.0.1',
    description="{Add a description for your package}",
    url='https://github.com/energyaspects/{add your package git url}.git',
    author='EA Data Engineering Team',
    author_email='data-engineering@energyaspects.com',
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires='>=3.9',
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.9",
    ],
    install_requires=REQUIREMENTS,
    package_data={"": ["*"]},
    # if you are using jsons or csvs for your code then you need to include this to make sure that the package includes these data when it installs
    entry_points={
        'console_scripts': [
            "name_of_task = india_mopng_etl.main:main",
            # these are the commands that can be ran through bash. Make sure you specify the correct folder under the
            # src path that includes the python script that you will run. In this instance it would be india_mopng_etl.main: main
        ],
    },

)
