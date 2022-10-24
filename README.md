# App Engine Scheduler

This app engine application is designed to retrieve data from an oracle database on a schedule and pipeline the data to big query. Due to long running queries (over an hour) cloud function and cloud run could not be used. When using a backend app engine instance scripts may run for up to 24 hours. These queries each take between 1 and 4 hours. This script is kept cost effective by having the scheduled scripts straddle the free-tier reset time of midnight. Short running queries are stacked, one particularly long running query starts on Saturday and runs through into Sunday. All scripts start after close of business Friday and complete before open of business Monday. 

For you to run this script you will need to create an "env.py" script in the functions folder. This script will contain your oracle (or other) connection file in the following format:

```
import oracledb
conn = oracledb.connect(
            user='username in quotes',
            password='password in quotes',
            host='hostname can be the webaddress or ip address of the service (in quotes)',
            port=1521,
            service_name='servicename in quotes'
      )

```
