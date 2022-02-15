import requests
import os

class Utils:

    @staticmethod
    def get_data_url(date_in, date_out):
        r = requests.get(f'https://opendata.paris.fr/api/v2/catalog/datasets/comptages-routiers-permanents/records?where=t_1h IN [date\'{date_in}\'..date\'{date_out}\']')
        json = r.text
        print(json)
        return json

    @staticmethod
    def whrite_json(file_path, name_file, data):
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        f = open(file_path + name_file, "w")
        f.write(data)
        f.close()

