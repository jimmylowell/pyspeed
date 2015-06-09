from helpers.postgres import commitCommand
from helpers import devices
from config_file import pg_config

import logging
module_logger = logging.getLogger("pyspeed.tables.servergeo")

table_base = """ (
pk BIGSERIAL PRIMARY KEY,
client_ip varchar,
isp varchar,
test_date timestamp,
download_kbps INTEGER,
upload_kbps INTEGER,
latency INTEGER,
latitude decimal,
longitude decimal,
geom GEOMETRY(POINT, 4326),"""

mobile_table = table_base + """
connection_type VARCHAR,
device VARCHAR)"""

web_table = table_base + """
client_city VARCHAR, 
client_region VARCHAR, 
client_country VARCHAR, 
client_browser VARCHAR, 
client_operating_system VARCHAR, 
user_agent VARCHAR)"""

def makeInsert(server_name, server_id, device, simple_sponsor, startdate, enddate):
	if device in 'web_browser':
		SERVER_INSERT = """INSERT INTO geo_""" + str(server_id) + """_web_browser
		(client_ip, isp, test_date,
		download_kbps, upload_kbps, latency, latitude, longitude,
		geom, user_agent)
		SELECT "CLIENT_IP", "ISP", "TEST_DATE",
		"DOWNLOAD_KBPS", "UPLOAD_KBPS", "LATENCY", "LATITUDE", "LONGITUDE",
		ST_SetSRID(ST_MakePoint("LONGITUDE", "LATITUDE"), 4326), "USER_AGENT"
		FROM temp_""" + simple_sponsor + """web_browser
		WHERE "SERVER_NAME" LIKE """ + server_name
		SERVER_INSERT+=""" and "TEST_DATE" BETWEEN '""" + str(startdate) + "' and '" + str(enddate) + "'"
		SERVER_INSERT += """ ORDER BY "TEST_DATE"; """
	else:
		SERVER_INSERT = """INSERT INTO geo_""" + str(server_id) + """_mobile
		(client_ip, isp, test_date,
		download_kbps, upload_kbps, latency, latitude, longitude,
		geom, connection_type, device)
		SELECT * FROM (
		SELECT "CLIENT_IP", "ISP", "TEST_DATE",
		"DOWNLOAD_KBPS", "UPLOAD_KBPS", "LATENCY", "LATITUDE", "LONGITUDE",
		ST_SetSRID(ST_MakePoint("LONGITUDE", "LATITUDE"), 4326), "CONNECTION_TYPE",
		'android' AS device
		FROM temp_""" + simple_sponsor + """android"""
		SERVER_INSERT += """ WHERE "SERVER_NAME" LIKE
		"""
		SERVER_INSERT += server_name
		SERVER_INSERT += """ and "TEST_DATE" BETWEEN '""" + str(startdate) + "' and '" + str(enddate) + "'"
		SERVER_INSERT += """
		UNION
		SELECT "CLIENT_IP", "ISP", "TEST_DATE",
		"DOWNLOAD_KBPS", "UPLOAD_KBPS", "LATENCY", "LATITUDE", "LONGITUDE",
		ST_SetSRID(ST_MakePoint("LONGITUDE", "LATITUDE"), 4326), "CONNECTION_TYPE",
		'iphone' AS device
		FROM temp_""" + simple_sponsor + """iphone"""
		SERVER_INSERT += """ WHERE "SERVER_NAME" LIKE
		"""
		SERVER_INSERT += server_name
		SERVER_INSERT += """ and "TEST_DATE" BETWEEN '""" + str(startdate) + "' and '" + str(enddate) + "'"
		SERVER_INSERT += """
		UNION
		SELECT "CLIENT_IP", "ISP", "TEST_DATE",
		"DOWNLOAD_KBPS", "UPLOAD_KBPS", "LATENCY", "LATITUDE", "LONGITUDE",
		ST_SetSRID(ST_MakePoint("LONGITUDE", "LATITUDE"), 4326), "CONNECTION_TYPE",
		'windows_phone' AS device
		FROM temp_""" + simple_sponsor + """windows_phone"""
		SERVER_INSERT += """ WHERE "SERVER_NAME" LIKE
		"""
		SERVER_INSERT += server_name
		SERVER_INSERT += """ and "TEST_DATE" BETWEEN '""" + str(startdate) + "' and '" + str(enddate) + "'"
		SERVER_INSERT += """ ) mobile
		ORDER BY mobile."TEST_DATE" """
# 	print SERVER_INSERT
	return SERVER_INSERT

def drop(sponsor_string, server_id):
		logger = logging.getLogger("pyspeed.Server.dropGeo")
		logger.info("Dropping geo tables for: " + sponsor_string)
		for device in ['web_browser', 'mobile']:
			if device[0] in 'web_browser':
				cmd = "DROP TABLE public.geo_" + str(server_id) + "_web_browser CASCADE"
				statusmessage = commitCommand(cmd)
				logger.debug(statusmessage + " " + str(server_id) + "_" + device[0])
			else:
				cmd = "DROP TABLE public.geo_" + str(server_id) + "_mobile CASCADE"
				print cmd
				statusmessage = commitCommand(cmd)

def create(sponsor_string, server_id):
	logger = logging.getLogger("pyspeed.Server.createGeo")
	logger.info("Creating tables for: " + str(sponsor_string))
	for device in ['web_browser', 'mobile']:
# 			device = device[0]
		if device in 'web_browser':
			cmd = "create table public.geo_"  + str(server_id) + "_web_browser" + web_table
			statusmessage = commitCommand(cmd)
			logger.debug(statusmessage + " " + str(server_id) + "_" + device)
		else:
			cmd = "create table public.geo_"  + str(server_id) + "_mobile" + mobile_table
			statusmessage = commitCommand(cmd)
			logger.debug(statusmessage + " " + str(server_id) + "_" + device)
