import psycopg2
from config_file import pg_config


pg_host = pg_config.get('pg_host')
pg_database = pg_config.get('pg_database')
pg_user = pg_config.get('pg_user')
pg_password = pg_config.get('pg_password')

def commitCommand(cmd):
	con = psycopg2.connect(host=pg_host,database=pg_database, user=pg_user, password=pg_password) 
	cur = con.cursor()
	cur.execute(cmd)
	con.commit()
	con.close()
	print cur.statusmessage
	return cur.statusmessage