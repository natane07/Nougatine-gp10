from utils import utils
import datetime


def script_get_data_alld_day():
    date_in = datetime.date.today() - datetime.timedelta(days=1)
    month = date_in.month
    year = date_in.year
    print(month, year)
    date_in = f'{date_in}T00:00'
    date_out = datetime.date.today()
    date_out = f'{date_out}T00:00'
    data = utils.Utils.get_data_url(date_in, date_out)
    utils.Utils.whrite_json(f'./data/clean/{year}/{month}/', f'circulation_{date_in}.json', data)

if __name__ == '__main__':
    script_get_data_alld_day()

