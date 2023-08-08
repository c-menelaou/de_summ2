import yaml
import os 
import sys 
import json
import datetime
import pytz

from resources import *


def etl_0(cutoff_date, full_refresh=False):
    idx = 0
    exist_data = []
    if not full_refresh:
        with open(cfg['l0_path'],'r') as f:
            for line in f:
                exist_data.append(line.split(',')[0])

    files = os.listdir(cfg['source_data_dir'])
    for file in files:

        with open(os.path.join(cfg['source_data_dir'], file), 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
            except:
                print(file)

        encounters = (x for x in data['entry'] if x['resource']['resourceType']=='Encounter')
        medications = (x for x in data['entry'] if x['resource']['resourceType']=='MedicationOrder')
        patients = (x for x in data['entry'] if x['resource']['resourceType']=='Patient')
        conditions = (x for x in data['entry'] if x['resource']['resourceType']=='Condition')

        for enc_data in encounters:
            e = Encounter(enc_data['resource'])
            if e.id in exist_data:
                continue
            if e.startTime < cutoff_date:
                if (idx == 0) and full_refresh:
                    e.to_csv(cfg['l0_path'], 'w') 
                else:
                    e.to_csv(cfg['l0_path'], 'a')
                idx += 1

def etl_1(data_dt):
    # Second file with data_dt
    i = 0
    l1_fp = os.path.join(cfg['output_data_dir'], 'tmp_encs.csv')
    l2_fp = os.path.join(cfg['output_data_dir'], 'tmp_encs2.csv')
    with open(l1_fp, 'r') as f_in:
        with open(l2_fp, 'w') as f_out:
            for line in f_in:
                l = line.rstrip('\n')
                f_out.write(f'{l},{data_dt}'+'\n')
                i += 1


if __name__ == "__main__":
    # Development variables, remove or comment when done:
    dev = True

    with open('/home/chrismenelaou/de/cfg.yaml', 'r') as cfg_f:
        cfg = yaml.load(cfg_f, yaml.SafeLoader)


    if dev == True:
        data_dt = '2010-03-01'
        cutoff_date  = datetime.datetime.fromisoformat(data_dt).astimezone(pytz.timezone('US/Eastern'))

    idx = 0
    full_refresh = True

    etl_0(cutoff_date=cutoff_date, full_refresh=full_refresh)
    # etl_1(data_dt)
