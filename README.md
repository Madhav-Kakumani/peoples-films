# peoples-films

## Setup
Below are the instructions to do local build.

1. git clone repository
2. Install IDE: pycharm
3. Install python 3.7 and update the python path in profile file
4. Open the project in pycharm
5. Using Terminal create virtual environment and activate it
6. Install pipenv
7. Using pipenv - install the libraries in Pipfile
   - pipenv install --dev (installs everything under [dev-packages])
8. Point preference > project > python interpreter to virtual environment

## Project structure
people
|__ingest: contains all common methods used to call people API, load and transform/aggregate data in mysql tables
|__script: contains script to call the methods to load raw data into mysql table and table to capture old character by film

## Script execution
1. Using pycharm IDE - Run > Choose 'launch_people_api.py' 
