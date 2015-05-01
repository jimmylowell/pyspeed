import xml.etree.ElementTree as ET
import psycopg2
# import sys
# from requests import session
from config_file import pg_config

pg_host = pg_config.get('pg_host')
pg_database = pg_config.get('pg_database')
pg_user = pg_config.get('pg_user')
pg_password = pg_config.get('pg_password')

# insert_cmd = """INSERT INTO speedtest_servers
# (server_id, sponsor_name, 
# latitude, longitude, name, 
# country, cc,
# url, url2, host)
# VALUES
# ("""

create_cmd = """CREATE TABLE public.speedtest_servers
(
  server_id smallint NOT NULL,
  sponsor_name character varying,
  latitude numeric,
  longitude numeric,
  name character varying,
  country character varying,
  cc character varying(2),
  url character varying,
  url2 character varying,
  host character varying,
  CONSTRAINT "server_id_PK" PRIMARY KEY (server_id)
)"""

insert_cmd = """INSERT INTO public.speedtest_servers
(server_id, sponsor_name, 
name 
)
VALUES
("""
print insert_cmd
test = '(server_id, sponsor_name, latitude, longitude,name, country, cc,url, url2, host)'


# with session() as s:
# 	get = s.get('http://c.speedtest.net/speedtest-servers-static.php')
# 	speedtest_servers = get.text
# 	file_ = codecs.open('speedtest_servers.xml', 'w', 'utf-8')
# 	file_.write(speedtest_servers)
# 	file_.close()

def quoteString(string):
	new_string = "'" + string + "'"
	return new_string

def dropServerTable():
	con = psycopg2.connect(host=pg_host,database=pg_database, user=pg_user, password=pg_password) 
	cur = con.cursor()
	cur.execute('DROP TABLE public.speedtest_servers')
	con.commit()
	con.close()
	
def createServerTable():
	try:
		con = psycopg2.connect(host=pg_host,database=pg_database, user=pg_user, password=pg_password) 
		cur = con.cursor()
		cur.execute(create_cmd)
		con.commit()
		print cur.statusmessage
	except:
		print cur.statusmessage
		print "ERROR"
		dropServerTable()
		cur.execute(create_cmd)
	con.commit()
	parseServerXML()
	con.close()

def parseServerXML():
	server_file = open('speedtest_servers.xml', 'r')
	tree = ET.parse(server_file)	
	root = tree.getroot()
	for server in root.iter('server'):
		url = quoteString(str(server.get('url')))
		lat = str(server.get('lat'))
		lon = str(server.get('lon'))
		name = server.get('name')
		country = str(server.get('country'))
		cc = str(server.get('cc'))
		sponsor = server.get('sponsor')
		serverid = str(server.get('id'))
		url2 = str(server.get('url2'))
		host = str(server.get('host'))
# 			print name
# 			print str(serverid), sponsor, str(lat), str(lon), name, country, cc, url, url2, host + ")"
		cmd = insert_cmd + serverid +"," + quoteString(sponsor) + "," + quoteString(name) + ")"
# 		print cmd
		try:
			con = psycopg2.connect(host=pg_host,database=pg_database, user=pg_user, password=pg_password) 
			cur = con.cursor()
			cur.execute(cmd)
			print cur.statusmessage
			con.commit()
			con.close()
		except:
			print cur.statusmessage
			con.close()
			print "ERROR"
			
if __name__ == "__main__":
	createServerTable()