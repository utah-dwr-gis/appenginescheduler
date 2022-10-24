from __future__ import print_function, absolute_import #package to smooth over python 2 and 3 differences
from datetime import datetime, timedelta
import logging
import flask
from flask import request
from google.cloud import bigquery
import function.natureCentroidCitation as natureCentroidCitation #the subfolder structure for calling the script
import function.natureCentroidCitationSF as natureCentroidCitationSF 
import function.natureCentroidEO as natureCentroidEO 
import function.natureCentroidEOcross as natureCentroidEOcross 
import function.natureCentroid as natureCentroid
import function.naturePoint as naturePoint
import function.naturePoly as naturePoly
import function.natureLine as natureLine
import os

app = flask.Flask(__name__)
@app.route('/')
def nature():
    return "Version 1 written by william wiskes 9/29/22 published under MIT"


@app.route('/natureCentroidCitation') 
def start_natureCentroidCitation():
    # # to limit access uncomment this code and place in each invocation
    # is_cron = request.headers.get('X-Appengine-Cron', False)
    # if not is_cron:
    #     return 'Bad Request', 400

    try:
        natureCentroidCitation.run() 
        return "Data copied to bigquery successfully", 200
    except Exception as e:
        logging.exception(e)
        return "Error: <pre>{}</pre>".format(e), 500

@app.route('/natureCentroidCitationSF') 
def start_natureCentroidCitationSF():
    try:
        natureCentroidCitationSF.run() 
        return "Data copied to bigquery successfully", 200
    except Exception as e:
        logging.exception(e)
        return "Error: <pre>{}</pre>".format(e), 500
        
@app.route('/natureCentroidEO') 
def start_natureCentroidEO(): 
    try:
        natureCentroidEO.run() 
        return "Data copied to bigquery successfully", 200
    except Exception as e:
        logging.exception(e)
        return "Error: <pre>{}</pre>".format(e), 500

@app.route('/natureCentroidEOcross') 
def start_natureCentroidEOcross(): 
    try:
        natureCentroidEOcross.run() 
        return "Data copied to bigquery successfully", 200
    except Exception as e:
        logging.exception(e)
        return "Error: <pre>{}</pre>".format(e), 500

@app.route('/natureCentroid')
def start_natureCentroid(): 
    try:
        natureCentroid.run() 
        return "Data copied to bigquery successfully", 200
    except Exception as e:
        logging.exception(e)
        return "Error: <pre>{}</pre>".format(e), 500

@app.route('/naturePoint')
def start_naturePoint(): 
    try:
        naturePoint.run() 
        return "Data copied to bigquery successfully", 200
    except Exception as e:
        logging.exception(e)
        return "Error: <pre>{}</pre>".format(e), 500

@app.route('/naturePoly')
def start_naturePoly(): 
    try:
        naturePoly.run() 
        return "Data copied to bigquery successfully", 200
    except Exception as e:
        logging.exception(e)
        return "Error: <pre>{}</pre>".format(e), 500

@app.route('/natureLine')
def start_natureLine():
    try:
        natureLine.run() 
        return "Data copied to bigquery successfully", 200
    except Exception as e:
        logging.exception(e)
        return "Error: <pre>{}</pre>".format(e), 500


@app.errorhandler(500) #error handling script for troubleshooting
def server_error(e):
    logging.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500


if __name__ == '__main__':
    app.run(debug=True, port=os.getenv("PORT", default=5000))