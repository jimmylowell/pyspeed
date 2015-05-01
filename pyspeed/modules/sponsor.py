#!/usr/bin/python
from requests import session
import errno
import datetime
import sys
import os
import psycopg2
import logging
import glob

from modules.tables import sponsortemp
from modules.tables import servergeo

from config_file import pg_config
from helpers import devices
from helpers import dates
from datetime import timedelta
reload(sys)
from modules.server import Server
module_logger = logging.getLogger("pyspeed.SpeedtestSponsor")

pg_host = pg_config.get('pg_host')
pg_database = pg_config.get('pg_database')
pg_user = pg_config.get('pg_user')
pg_password = pg_config.get('pg_password')

sponsor_select = """
SELECT server_id 
FROM speedtest_servers
WHERE sponsor_name 
LIKE """


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:20.0) Gecko/20100101 Firefox/20.0",
    "Accept-Encoding": "gzip, deflate",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive"
}

daylight_savings = ['2011-03-13', '2011-11-06', '2012-03-11', '2012-11-04', '2013-03-10', '2013-11-03', '2014-03-09', '2014-11-02', '2015-03-08']
day_after_daylight_savings = ['2011-03-14', '2011-11-07', '2012-04-12', '2012-11-05', '2013-03-11', '2013-11-04', '2014-03-10', '2014-11-03', '2015-03-09']

class SpeedtestSponsor(object):
	
	def __init__(self, sponsor_string, first_test_date,
				 username, password
				 ):
		self.sponsor_string = sponsor_string
		self.first_test_date = first_test_date
		self.username = username
		self.password = password
		self.simple_name = self.getSimpleNameFromEmail()
		self.servers = [Server(self.sponsor_string, server_id) for server_id in self.getServerIDs()]
		logger = logging.getLogger("pyspeed.SpeedtestSponsor.init")
		logger.info("sponsor instantiated")
				
	def getServerIDs(self):
		query = sponsor_select + "'" + self.sponsor_string + "'"
		con = psycopg2.connect(host=pg_host,database=pg_database, user=pg_user, password=pg_password) 
		cur = con.cursor()
		cur.execute(query)
		server_list = cur.fetchall()
		for server_id in server_list:
			yield server_id[0]
			
	def getSimpleNameFromEmail(self):
		email = self.username.split('@')[1]
		simple_name = email.split('.')[0]
		return simple_name
	
	def getLastGeoTestFromPG(self):
		print "Getting last test result in PG for: " + self.sponsor_string
		con = psycopg2.connect(host=pg_host,database=pg_database, user=pg_user, password=pg_password) 
		cur = con.cursor()
		first_test = self.first_test_date
		last_test = datetime.datetime.strptime(first_test, '%Y-%m-%d') - timedelta(days=1)
		for server in self.servers:
			QUERY = """
					SELECT test_date FROM
					public.geo_""" + str(server.server_id) + """_web_browser
					UNION
					SELECT test_date FROM
					public.geo_""" + str(server.server_id) + """_mobile
					ORDER BY test_date DESC LIMIT 1"""
			try:
				cur.execute(QUERY)
				for test_date in cur.fetchall():
					last_test_temp = test_date[0]
	# 				print last_test_temp
					if last_test_temp > last_test:
						last_test = last_test_temp
			except:
				print "USING FIRST TEST DATE"
		return last_test
	
	def downloadMissingTSVs(self, end_date_range):
		print "FINDING MISSING TSVs for: " + self.sponsor_string
		tsv_dir = './tsv/' + self.simple_name + '/'
# 		print tsv_dir
		try:
			tsvs = glob.glob('./tsv/' + self.simple_name + '/' + "web_browser*.tsv")
			tsvs.sort(reverse=True)
			last_file = tsvs[0]
			last_date = last_file.split('--')[1]
			last_date = last_date.split('.tsv')[0]
		except:
			os.mkdir(tsv_dir)
			last_date = self.first_test_date
		print last_date
		self.loginDownloadTSVs(last_date, end_date_range)		
														
	def loginDownloadTSVs(self, start_date_range, end_date_range):
		logger = logging.getLogger("pyspeed.SpeedtestSponsor.loginDownloadTSVs")
		logger.info("Starting to download TSVs from "+str(start_date_range) + " to " + str(end_date_range))	
		speedtest_user = self.username
		speedtest_password = self.password
		save_dir = 'tsv/' + self.simple_name + '/'
		logger.debug("Using directory for tsvs: " + save_dir)
# 		print save_dir
		with session() as s:
			logger.debug("Logging in with user: " + speedtest_user)
			payload = make_payload(speedtest_user, speedtest_password)
			post = s.post('http://reporting.speedtest.net/login.php', data=payload, headers=headers)
			response=post.text
			good_response = 'Logged in as <span class="login-username">' + speedtest_user.lower()
			logger.debug("Checking login status by searching for " +  "'" + good_response + "'")
# 			print response
			response.index(good_response)
			logger.debug("Success - Logged in with user: " + speedtest_user)		
			for date in dates.daterange( dates.make(start_date_range), dates.make(end_date_range)):
				str_date = str(date)
# 				print str_date
				if( str_date in daylight_savings):
					for device in devices.yieldAll():
						getOneTSV(date, date + datetime.timedelta(days=1), device, s, save_dir)
				elif( str_date in day_after_daylight_savings):
					print 'skipping'
				else:
					for device in devices.yieldAll():
						getOneTSV(date, date, device, s, save_dir)
						
	def bulkLoadTSVs(self, start_date_range, end_date_range):
		logger = logging.getLogger("pyspeed.SpeedtestSponsor.bulkLoadTSVs")
		logger.info("Starting bulk load TSV for " + start_date_range + " to " + end_date_range)
# 		Drop/Create tables for bulk load
		for server in self.servers:
			try:
				servergeo.create(server.sponsor_string, server.server_id)
			except:
				servergeo.drop(server.sponsor_string, server.server_id)
				servergeo.create(server.sponsor_string, server.server_id)
		self.loadTSVs(start_date_range, end_date_range)
		
	def updateLoadTSVs(self, end_date_range):
		print end_date_range
		for server in self.servers:
			print server.server_name
			try:
				servergeo.create(server.sponsor_string, server.server_id)
			except:
				print "GEO EXISTS, NOT DROPPING"
		last_test = self.getLastGeoTestFromPG()
		print last_test
		start_date = last_test + timedelta(days=1)
		print "TESTING DATES"
		if start_date.date() == datetime.date.today():
			print 'NONE TO UPDATE'
		else:
			start_date_range = start_date.date()
			print start_date_range
			self.loadTSVs(str(start_date_range), end_date_range)
	
	def loadTSVs(self, start_date_range, end_date_range):
		save_dir = self.simple_name
		for date in dates.daterange(dates.make(start_date_range), dates.make(end_date_range)):
			print str(date)
			if( str(date) in daylight_savings):
				sponsortemp.create(self.simple_name)
				for device in devices.listNames():
					sponsortemp.insert(date, date + datetime.timedelta(days=1), device, save_dir)
				for server in self.servers:
					server.insertTests(self.simple_name, date, date + datetime.timedelta(days=2))
				self.dropTemp()
			elif( str(date) in day_after_daylight_savings):
				print 'Skipping ' + str(date) + ' for daylight savings fix'
			else:
				try:
					sponsortemp.create(self.simple_name)
				except:
					sponsortemp.drop(self.simple_name)
					sponsortemp.create(self.simple_name)
				for device in devices.listNames():
					sponsortemp.insert(date, date, device, save_dir)
				for server in self.servers:
					server.insertTests(self.simple_name, date, date + datetime.timedelta(days=1))
				sponsortemp.drop(self.simple_name)
	
def make_payload(speedtest_user, speedtest_password):
	payload = {
	'action': 'login',
	'logintype': 'host',
	'username': speedtest_user,
	'password': speedtest_password}
	return payload

def getOneTSV( start_date, end_date, device, s, save_dir):
	logger = logging.getLogger("pyspeed.SpeedtestSponsor.getOneTSV")
	start=str(start_date)
	end=str(end_date)
	device_name = device[0]
	device_id = device[1]
	tsv = save_dir+device_name+'_'+start+'--'+end+'.tsv'
	logger.debug("location to save file: " + tsv)
	tsvurl = 'http://reporting.speedtest.net/csv.php?type='+device_id+'&downloadtype=date&start_date='+start+'&end_date='+end+'&separator=Download+in+TAB+format'
	results = s.get(tsvurl, headers=headers)
	a = open(tsv, 'w')
	test = results.text
	utf = test.encode('UTF-8')
	a.write(utf)
	a.close()
	tsv_kb = os.path.getsize(tsv)/1024
	logger.debug("Saved file size: " + str(tsv_kb) + 'KB')		