from __future__ import annotations

import logging
import shutil
import sys
import os
import tempfile
import time
from pprint import pprint
import yaml
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook

log = logging.getLogger(__name__)

dev_mode = False

path = '/home/chrismenelaou/de'
if path not in sys.path:
  sys.path.append(path)

with DAG(
  dag_id="ehr_etl_pipeline",
  schedule="0 12 1 * *",
  start_date=pendulum.datetime(2010,1,1,tz="UTC"),
  catchup=False,
  tags=['data_engineering'],
) as dag:
  
  with open(os.path.join(path,'cfg.yaml'), 'r') as cfg_f:
    cfg = yaml.load(cfg_f, yaml.SafeLoader)

  # Define task functions:
  @task(task_id="start_up")
  def start_up(**kwargs):
    from resources import MedicationOrder, Encounter, Patient, Condition, make_csv_header
    make_csv_header(obj=MedicationOrder, filepath=cfg['tmp_path']['MedicationOrder'])
    make_csv_header(obj=Encounter, filepath=cfg['tmp_path']['Encounter'])
    make_csv_header(obj=Patient, filepath=cfg['tmp_path']['Patient'])
    make_csv_header(obj=Condition, filepath=cfg['tmp_path']['Condition'])
    return "Starting Up"

  def extract_function(data_type, ti=None, **kwargs):
    import pipeline
    csv_str_w_date = pipeline.main(dev=dev_mode, data_dt=kwargs['ts'], object_type=data_type)

  def load_function(data_type, table_name, ti=None, **kwargs):
    mysql_hook = MySqlHook('mysql_ehr', schema='ehr_db', local_infile=True)
    print(data_type)
    print(cfg['tmp_path'][data_type])
    mysql_hook.bulk_load_custom(tmp_file=cfg['tmp_path'][data_type], table=table_name, duplicate_key_handling='IGNORE')

  @task(task_id="cleanup")
  def cleanup(**kwargs):
    os.remove(cfg['tmp_path']['MedicationOrder'])
    os.remove(cfg['tmp_path']['Patient'])
    os.remove(cfg['tmp_path']['Condition'])
    os.remove(cfg['tmp_path']['Encounter'])




  # Create tasks:
  # =============
  startup_task = start_up()

  encounters_extract_task = task(task_id='extract_encounters')(extract_function)(data_type='Encounter')
  encounters_load_task = task(task_id='load_encounters')(load_function)(data_type='Encounter', table_name='encounters')

  medications_extract_task = task(task_id='extract_medication_orders')(extract_function)(data_type='MedicationOrder')
  medications_load_task = task(task_id='load_medication_orders')(load_function)(data_type='MedicationOrder', table_name='medications')

  patients_extract_task = task(task_id='extract_patients')(extract_function)(data_type='Patient')
  patients_load_task = task(task_id='load_patients')(load_function)(data_type='Patient', table_name='patients')

  conditions_extract_task = task(task_id='extract_conditions')(extract_function)(data_type='Condition')
  conditions_load_task = task(task_id='load_conditions')(load_function)(data_type='Condition', table_name='conditions')

  cleanup_task = cleanup()

  # Define DAG structure:
  # =====================
  # Start-up to extract
  startup_task >> [encounters_extract_task, medications_extract_task, patients_extract_task, conditions_extract_task]
  # Extract to load
  medications_extract_task >> medications_load_task
  encounters_extract_task >> encounters_load_task
  patients_extract_task >> patients_load_task
  conditions_extract_task >> conditions_load_task
  # Load to clean-up
  [encounters_load_task, medications_load_task, conditions_load_task, patients_load_task] >> cleanup_task

