from modules.table import Table

def makeMobileSelectCMD(server_id):
	cmd="""SELECT
CONCAT(pk, '-', """ + str(server_id)+""") AS mobile_uid,
""" + str(server_id) + """ AS server_id, pk AS server_mobile_pk,
download_kbps, upload_kbps, latency,
test_date,
isp, geom, device
FROM
public.geo_""" + str(server_id) + """_mobile
"""
	return cmd
	
def makeMobileTableCMD(sponsors):
	cmd = ""
	for sponsor in sponsors[:-1]:
		for server_id in sponsor.getServerIDs():
			cmd += makeMobileSelectCMD(server_id) + """
			UNION ALL
			"""
	for server_id in sponsors[-1].getServerIDs():
		cmd += makeMobileSelectCMD(server_id)
	return cmd

def makeBrowserSelectCMD(server_id):
	cmd="""SELECT
CONCAT(pk, '-', """ + str(server_id)+""") AS browser_uid,
""" + str(server_id) + """ AS server_id, pk AS server_browser_pk,
download_kbps, upload_kbps, latency,
test_date,
isp, geom
FROM
public.geo_""" + str(server_id) + """_web_browser
"""
	return cmd
	
def makeBrowserTableCMD(sponsors):
	cmd = ""
	for sponsor in sponsors[:-1]:
		for server_id in sponsor.getServerIDs():
			cmd += makeBrowserSelectCMD(server_id) + """
			UNION ALL
			"""
	for server_id in sponsors[-1].getServerIDs():
		cmd += makeBrowserSelectCMD(server_id)
	return cmd
	
class Mobile(Table):
	
	def __init__(self):
		self.table_name = 'olap.mobile'
			
	def makeTable(self, sponsors):
		self.drop()
		self.createTableAs(makeMobileTableCMD(sponsors))
		self.addPrimaryKey('mobile_uid')
		self.buildIndex("test_date")

class Browser(Table):
	
	def __init__(self):
		self.table_name = 'olap.browser'
			
	def makeTable(self, sponsors):
		self.drop()
		self.createTableAs(makeBrowserTableCMD(sponsors))
		self.addPrimaryKey('browser_uid')
		self.buildIndex('test_date')
	