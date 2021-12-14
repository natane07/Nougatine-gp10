import requests

class Utils:

    @staticmethod
    def get_data_url(url):
        r = requests.get("https://opendata.paris.fr/api/v2/catalog/datasets/comptages-routiers-permanents/records?where=t_1h IN [date'2021-12-12T00:00'..date'2021-12-13T00:00']")
        json = r.json()
        print(json)