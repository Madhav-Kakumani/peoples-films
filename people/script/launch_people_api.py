# script to call people api
from people.ingest.people_download_api import call_api
from people.ingest.people_download_api import load_people_data
from people.ingest.people_download_api import transform_aggregate_people

if __name__ == "__main__":
    people_list = call_api()
    load_success = load_people_data(people_list)
    if load_success == 'True':
        transform_aggregate_people()
