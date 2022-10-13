
from google.cloud import bigquery
import flask
import json
from flask import jsonify
import oracledb
import os
import logging #package for error logging
from env import * # you must make an env.py file with your oracle.db connection named as "conn"

#tracks error messaging
logging.basicConfig(level=logging.INFO)

def run():
    # conn = oracledb.connect(
    #         the actual connection is found in env.py
    # )
    curs = conn.cursor()
    sql = '''
SELECT source_feature_id, TO_CHAR(SDO_UTIL.TO_WKTGEOMETRY(shape)) as geom FROM SOURCE_FEATURE_PRE_PT
'''
    curs.execute(sql)
    columns = [col[0] for col in curs.description]
    curs.rowfactory = lambda *args: dict(zip(columns, args))
    gcp = curs.fetchall()

    PROJECT_ID = 'ut-dnr-biobase-dev'
    client = bigquery.Client(project=PROJECT_ID, location="US")
    # set location 
    dataset_id = 'biobase'
    table_id = 'naturePoint'
    # set config
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job = client.load_table_from_json(
          gcp, table_ref, job_config=job_config
    ) 
    job.result()  # Wait for the job to complete.
    # result = "Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id)
      
    curs.close()
    conn.close()
    