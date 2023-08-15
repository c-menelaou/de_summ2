import yaml
import os 
import sys 
import json
import datetime
import pytz
from tqdm import tqdm
import dotenv
import mysql.connector
from io import StringIO

from resources import *

objects = [Encounter, MedicationOrder, Patient, Condition]
_ = dotenv.load_dotenv()

objects = {
    'Encounter':Encounter,
    'MedicationOrder': MedicationOrder,
    'Patient': Patient,
    'Condition': Condition
}

relevant_dates = {
    'Encounter':'endTime',
    'MedicationOrder': 'dateWritten',
    'Patient': 'birthDate',
    'Condition': 'dateRecorded'
}

def extract_to_csv(
        object_type: str, 
        cfg: dict, 
        cutoff_date: str, 
        full_refresh: bool=False, 
        dev_mode:bool =False,
        sep='\t',
        header=False
    ):

    print('Running pipeline 1: Extract to CSV')

    # Pull the right object for this run
    Object = objects[object_type]
    # Initialise string buffer with column headings
    csv_str = StringIO()
    if header:
        csv_str.write(sep.join(Object.columns) + '\n')

    files = os.listdir(cfg['source_data_dir'])

    if dev_mode: 
        files_iterator = tqdm(files[:10])
    else:
        files_iterator = files
    
    for file in files_iterator:

        with open(os.path.join(cfg['source_data_dir'], file), 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
            except:
                print(file)

        items = (x for x in data['entry'] if x['resource']['resourceType']==object_type)

        for item in items:
            o = Object(item['resource'], today=cutoff_date)
            if date1_lt_date2(getattr(o, relevant_dates[object_type]), cutoff_date):
                o.to_csv(csv_str, sep=sep)
            
    return csv_str

def date1_lt_date2(date1, date2):
    if type(date1) == datetime.date:
        return date1 < date2.date()
    elif type(date1) == datetime.datetime:
        return date1 < date2


def append_date(csv_str_in, data_dt, sep, header=False, ti=None):
    # Second file with data_dt
    csv_str_out = StringIO()
    
    csv_str_in.seek(0)
    line = csv_str_in.readline()
    if header:
        csv_str_out.write(line.rstrip('\n') + sep + 'data_dt\n')

    for line in csv_str_in.readlines():
        csv_str_out.write(line.rstrip('\n') + sep + f'{data_dt}\n')
    

    return csv_str_out


def main(dev=False, data_dt=None, object_type=None, header=False):

    with open('/home/chrismenelaou/de/cfg.yaml', 'r') as cfg_f:
        cfg = yaml.load(cfg_f, yaml.SafeLoader)

    if dev == True:
        data_dt = '2010-03-01'

    cutoff_date  = datetime.datetime.fromisoformat(data_dt).astimezone(pytz.timezone('US/Eastern'))
    idx = 0
    full_refresh = True

    csv_str = extract_to_csv(
        object_type=object_type, 
        cfg=cfg, 
        cutoff_date=cutoff_date, 
        full_refresh=full_refresh, 
        dev_mode=dev,
        header=header
        )
    data_date = datetime.datetime.fromisoformat(data_dt).date()
    csv_str_w_date = append_date(csv_str, data_date, sep='\t', header=header)

    tmp_path = cfg['tmp_path'][object_type]
    with open(tmp_path, 'w') as f:
      f.write(csv_str_w_date.getvalue())
    
    return csv_str_w_date




if __name__ == "__main__":

    # Development variables, remove or comment when done:
    csv_str_w_date = main(dev=True)
    with open('./output_data/tmp.csv', 'w') as f:
        f.write(csv_str_w_date.getvalue())
    
