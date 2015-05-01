from helpers.postgres import commitCommand
from helpers import devices
from config_file import pg_config

import datetime
import psycopg2

pg_host = pg_config.get('pg_host')
pg_database = pg_config.get('pg_database')
pg_user = pg_config.get('pg_user')
pg_password = pg_config.get('pg_password')

temp__mobile_table = """ (
"CLIENT_IP" varchar,
"ISP" varchar,
"TEST_DATE" timestamp,
"SERVER_NAME" varchar,
"DOWNLOAD_KBPS" integer,
"UPLOAD_KBPS" integer,
"LATENCY" integer,
"LATITUDE" decimal,
"LONGITUDE" decimal,
"CONNECTION_TYPE" varchar)"""

temp_web_table = """ (
"CLIENT_IP" varchar, 
"CLIENT_CITY" varchar, 
"CLIENT_REGION" varchar, 
"CLIENT_COUNTRY" varchar, 
"ISP" varchar, 
"LATITUDE" decimal, 
"LONGITUDE" decimal, 
"TEST_DATE" timestamp, 
"SERVER_NAME" varchar, 
"DOWNLOAD_KBPS" integer, 
"UPLOAD_KBPS" integer, 
"LATENCY" integer, 
"CLIENT_BROWSER" varchar, 
"CLIENT_OPERATING_SYSTEM" varchar, 
"USER_AGENT" varchar)"""

def drop(simple_name):
# 	con = psycopg2.connect(host=pg_host,database=pg_database, user=pg_user, password=pg_password)
# 	cur = con.cursor()
	for device in devices.listNames():
		cmd = "DROP TABLE temp_" + simple_name + device
# 			print cmd
		commitCommand(cmd)
# 		cur.execute(cmd)
# 	con.commit()
# 	con.close()
# 		print 'dropped temp table'
	
def create(simple_name):
# 	con = psycopg2.connect(host=pg_host,database=pg_database, user=pg_user, password=pg_password)
# 	cur = con.cursor()
	for device in devices.yieldAll():
# 			print device
		if device[0] in 'web_browser':
			cmd = "create table temp_" + simple_name + device[0] + temp_web_table
			commitCommand(cmd)
# 			cur.execute("create table temp_" + self.simple_name + device[0] + temp_web_table)
# 				print cur.statusmessage
		else:
			cmd = "create table temp_" + simple_name + device[0] + temp__mobile_table
			commitCommand(cmd)
# 			cur.execute("create table temp_" + self.simple_name + device[0] + temp__mobile_table)
# 				print cur.statusmessage
# 	con.commit()
# 	con.close()
	
def insert( start_date, end_date, device, simple_sponsor ):
	print start_date
	print end_date
	con = psycopg2.connect(host=pg_host,database=pg_database, user=pg_user, password=pg_password)
# 	logger = logging.getLogger("pyspeed.SpeedtestSponsor.insert_temp")
	start=str(start_date)
	end=str(end_date)
	cur=con.cursor()
	save_dir = './tsv/' + simple_sponsor + '/'
	tsv = save_dir+device+'_'+start+'--'+end+'.tsv'
# 	logger.debug('File for copy: ' + tsv)
	tsvfile=open(tsv, 'r')
	copy = 'COPY temp_' + simple_sponsor + device + """ FROM STDIN WITH CSV HEADER DELIMITER AS E'\\t'"""
# 	logger.debug('Copy command: ' + "'" + copy + "'")
# 	print copy
	cur.copy_expert(copy, tsvfile)
	con.commit()
	countsql = 'SELECT COUNT(*) FROM temp_' + simple_sponsor + device + ' WHERE "TEST_DATE" BETWEEN ' + "'" + str(start_date) + "'" + ' AND ' + "'" + str(end_date+ datetime.timedelta(days=1)) + "'"
	cur.execute(countsql)
	con.commit()

