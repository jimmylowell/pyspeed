import psycopg2
import sys
import logging
from config_file import pg_config

from helpers.postgres import commitCommand
from modules.tables import servergeo

reload(sys)

module_logger = logging.getLogger("pyspeed.server")

pg_host = pg_config.get('pg_host')
pg_database = pg_config.get('pg_database')
pg_user = pg_config.get('pg_user')
pg_password = pg_config.get('pg_password')


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

class Server(object):
	
	def __init__(self, sponsor_string, server_id):
		self.sponsor_string = sponsor_string
		self.server_id = server_id
		self.server_name = self.getServerName()
		
	def getServerName(self):
		con = psycopg2.connect(host=pg_host,database=pg_database, user=pg_user, password=pg_password)
		cur = con.cursor()
		cmd = "SELECT name FROM speedtest_servers WHERE server_id = " + str(self.server_id)
		cur.execute(cmd)
		for server_name in cur.fetchall():
			return server_name[0]
		
	def insertTests(self, simple_sponsor, startdate, enddate):
		for device in ['web_browser', 'mobile']:
			cmd = servergeo.makeInsert("'" + self.server_name + "'", self.server_id, device, simple_sponsor, startdate, enddate)
			commitCommand(cmd)
			
# 	def dropGeo(self):
# 		logger = logging.getLogger("pyspeed.Server.dropGeo")
# 		logger.info("Dropping geo tables for: " + self.sponsor_string)
# 		con = psycopg2.connect(host=pg_host,database=pg_database, user=pg_user, password=pg_password)
# 		cur = con.cursor()
# 		for device in ['web_browser', 'mobile']:
# 			if device[0] in 'web_browser':
# 				cur.execute("DROP TABLE public.geo_" + str(self.server_id) + "_web_browser CASCADE")
# 				con.commit()
# 				logger.debug(cur.statusmessage + " " + str(self.server_id) + "_" + device[0])
# 			else:
# 				print "DROP TABLE geo_" + str(self.server_id) + "_mobile CASCADE"
# 				cur.execute("DROP TABLE public.geo_" + str(self.server_id) + "_mobile CASCADE")
# 				con.commit()
# 		con.close()
		
# 	def createGeo(self):
# 		logger = logging.getLogger("pyspeed.Server.createGeo")
# 		logger.info("Creating tables for: " + str(self.sponsor_string))
# 		con = psycopg2.connect(host=pg_host,database=pg_database, user=pg_user, password=pg_password)
# 		cur = con.cursor()
# 		for device in ['web_browser', 'mobile']:
# # 			device = device[0]
# 			if device in 'web_browser':
# 				cmd = "create table public.geo_"  + str(self.server_id) + "_web_browser" + web_table
# # 				print cmd
# 				cur.execute(cmd)
# 				logger.debug(cur.statusmessage + " " + str(self.server_id) + "_" + device)
# # 				print cur.statusmessage
# 				con.commit()
# 			else:
# 				cmd = "create table public.geo_"  + str(self.server_id) + "_mobile" + mobile_table
# # 				print cmd
# 				cur.execute(cmd)
# 				logger.debug(cur.statusmessage + " " + str(self.server_id) + "_" + device)
# # 				print cur.statusmessage
# 				con.commit()
# # 		con.commit()
# 		con.close()

	def makeIndex(self):
		INDEX_CMD = """CREATE INDEX ON public.geo_""" + str(self.server_id) + "_"
		con = psycopg2.connect(host=pg_host,database=pg_database, user=pg_user, password=pg_password)
		cur = con.cursor()
		for device in ['web_browser', 'mobile']:
			cmd = INDEX_CMD + device + " (test_date DESC NULLS FIRST);"
			cur.execute(cmd)
		con.commit()
		con.close

