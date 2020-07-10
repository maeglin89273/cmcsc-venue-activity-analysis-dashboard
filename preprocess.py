from datetime import datetime
import pandas as pd
REPLACE_KEY_TABLE = {'swim': 'swimming_pool',
                     'gym': 'gym'}

CURRENT_NUMBER_OF_PEOPLE = 0
MAX_NUMBER_OF_PEOPLE = 1

class ProcessedDataHelper:
    def __init__(self, timestamp, data):
        self.timestamp = timestamp
        self.data = data

    def get_current_rooms_number_of_people(self):
        return { room: self.data[room]['current_number_of_people'] for room in self.data}

    def get_current_number_of_people(self, room):
        return self.data[room]['current_number_of_people']

    def get_max_number_of_people(self, room):
        return self.data[room]['max_number_of_people']

    @property
    def current_number_of_people_df(self):
        # pandas is so smart for broadcasting !
        return pd.DataFrame(self.get_current_rooms_number_of_people(), index=[self.timestamp])

def raw_json_preprocess(json_data):
    new_data = {}
    for key in json_data:
        entry = json_data[key]
        new_entry = {'current_number_of_people': int(entry[CURRENT_NUMBER_OF_PEOPLE]),
                 'max_number_of_people': int(entry[MAX_NUMBER_OF_PEOPLE])
                 }
        new_data[REPLACE_KEY_TABLE[key]] = new_entry

    return ProcessedDataHelper(datetime.now(), new_data)