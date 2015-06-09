#!/usr/bin/python
from modules.sponsor import SpeedtestSponsor
from config_file import sponsor_list_config

from modules.tables import olap

from datetime import date, timedelta
import logging

logger = logging.getLogger("pyspeed")
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('./logs/' + str(date.today())+'.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)

yesterday = date.today() - timedelta(1)
str_yesterday = str(yesterday)

def initSponsor(sponsor_config):
	sponsor_string = sponsor_config.get('sponsor_string')
	first_test_date = sponsor_config.get('first_test_date')
	sponsor_email = sponsor_config.get('sponsor_email')
	sponsor_password = sponsor_config.get('sponsor_password')
	
	#Instantiate SpeedtestSponsor
	sponsor= SpeedtestSponsor(sponsor_string, first_test_date,
						 sponsor_email, sponsor_password)
	return sponsor

def main():
# add handler to logger object
	logger.addHandler(fh)
	logger.info("Program started")
	print "Starting"
	sponsors = [initSponsor(sponsor_config) for sponsor_config in sponsor_list_config]
	for sponsor in sponsors:		
		sponsor.downloadMissingTSVs(str_yesterday)	
			
		# PostgreSQL import requires PostGIS
		sponsor.updateLoadTSVs(str_yesterday)
	
	olap.Mobile().makeTable(sponsors)
	olap.Browser().makeTable(sponsors)
	
	olap.Mobile().ExportToFile('/home/jimmy/qgis/site/files/')
	olap.Browser().ExportToFile('/home/jimmy/qgis/site/files/')
		
	logger.info("Done!")
	print "Done!"
	
if __name__ == "__main__":
	main()