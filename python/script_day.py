from utils import utils
import datetime
from subprocess import PIPE, Popen

def script_get_data_alld_day():
    date_in = datetime.date.today() - datetime.timedelta(days=1)
    month = date_in.month
    year = date_in.year
    print(month, year)
    date_in_string = f'{date_in}T00:00'
    date_out = datetime.date.today()
    date_out_string = f'{date_out}T00:00'
    data = utils.Utils.get_data_url(date_in_string, date_out_string)
    utils.Utils.whrite_json(f'./data/clean/{year}/{month}/', f'circulation_{date_in}.json', data)
    put = Popen(["hdfs", "dfs", "-put", f'/root/groupe10/Nougatine-gp10/script_data/data/clean/{year}/{month}/circulation_{date_in}.json', f'/data/gp10/'], stdin=PIPE, bufsize=-1)
    put.communicate()

if __name__ == '__main__':
    script_get_data_alld_day()
