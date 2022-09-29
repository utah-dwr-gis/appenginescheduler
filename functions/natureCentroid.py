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
SELECT 
CENTROID_VW_SF.MASK_UTMX, 
CENTROID_VW_SF.MASK_UTMY, 
CENTROID_VW_SF.SF_ID, 
CENTROID_VW_SF.SF_SHAPE_ID, 
CENTROID_VW_SF.ISF, 
ROUND(CENTROID_VW_SF.LOCUNCERT, 0) "LOCUNCERT",
CENTROID_VW_SF.LOCUNCUNIT, 
CENTROID_VW_SF.EST_ID, 
CENTROID_VW_EGT.EGT_ID, 
CENTROID_VW_EGT.ENT_ID, 
CENTROID_VW_EGT.ELCODEBCD, 
CENTROID_VW_EGT.NAME_CAT,
CENTROID_VW_EGT.GNAME,
CENTROID_VW_EGT.SNAME,
CENTROID_VW_EGT.SCOMNAME,
CENTROID_VW_EGT.S_RANK,
CENTROID_VW_EGT.G_RANK,
CENTROID_VW_EGT.SPROT,

CENTROID_VW_USESA.USESA_STAT, 
CENTROID_VW_USESA.USESA_COM,
CENTROID_VW_WAP.WAP_STAT,

CENTROID_VW_SF.SFDESC,
CENTROID_VW_SF.SFLOC,
CENTROID_VW_SF.SF_SURVEY,
CENTROID_VW_SF.SF_FIRSTOBS,
CENTROID_VW_SF.SF_LASTOBS,

SUBSTR(CENTROID_VW_OBSERVATION.OBSDATE,1,254) AS VISITDATE1,
SUBSTR(CENTROID_VW_OBSERVATION.OBSDATE,255,254) AS VISITDATE2,
SUBSTR(CENTROID_VW_OBSERVATION.OBSERVER,1,254) AS VISITEDBY1,
SUBSTR(CENTROID_VW_OBSERVATION.OBSERVER,255,254) AS VISITEDBY2,
SUBSTR(CENTROID_VW_OBSERVATION.OBSDATA,1,254) AS VISITDATA1,
SUBSTR(CENTROID_VW_OBSERVATION.OBSDATA,255,254) AS VISITDATA2,
SUBSTR(CENTROID_VW_OBSERVATION.OBSDATA,509,254) AS VISITDATA3,
SUBSTR(CENTROID_VW_OBSERVATION.OBSDATA,763,254) AS VISITDATA4,
SUBSTR(CENTROID_VW_OBSERVATION.OBSDATA,1017,254) AS VISITDATA5,
SUBSTR(CENTROID_VW_OBSERVATION.OBSDATA,1271,254) AS VISITDATA6,
EO_SOURCE_FEATURE.EO_ID

FROM 
CENTROID_VW_SF, 
CENTROID_VW_EGT,
CENTROID_VW_USESA,
CENTROID_VW_OBSERVATION,
CENTROID_VW_WAP,
EO_SOURCE_FEATURE

WHERE
CENTROID_VW_EGT.EST_ID(+)=CENTROID_VW_SF.EST_ID
AND CENTROID_VW_SF.EST_ID=CENTROID_VW_USESA.EST_ID(+)
AND CENTROID_VW_SF.EST_ID=CENTROID_VW_WAP.EST_ID(+)
AND CENTROID_VW_SF.SF_ID=CENTROID_VW_OBSERVATION.SF_ID(+)
AND CENTROID_VW_SF.SF_SHAPE_ID(+)= EO_SOURCE_FEATURE.SOURCE_FEATURE_SHAPE_ID  
'''
    curs.execute(sql)
    columns = [col[0] for col in curs.description]
    curs.rowfactory = lambda *args: dict(zip(columns, args))
    gcp = curs.fetchall()

    PROJECT_ID = 'ut-dnr-biobase-dev'
    client = bigquery.Client(project=PROJECT_ID, location="US")
    # set location 
    dataset_id = 'biobase'
    table_id = 'natureCentroid'
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
    