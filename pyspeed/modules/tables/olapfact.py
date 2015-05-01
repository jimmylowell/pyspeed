from helpers.postgres import commitCommand
from modules.table import Table

def makeSelectCMD(server_id):
	cmd="""SELECT
CONCAT(pk, '-', """ + str(server_id)+""") AS uid,
""" + str(server_id) + """ AS server_id, pk,
download_kbps, upload_kbps, latency,
test_date,
isp, geom
FROM
public.geo_""" + str(server_id) + """_mobile
"""
	return cmd



def makeIndexesCMD():
# 	index_cmd = """ALTER TABLE olap.fact_mobile ADD PRIMARY KEY (uid);"""
	index_cmd = """CREATE INDEX ON olap.fact_mobile (test_date);"""
	index_cmd += """CREATE INDEX ON olap.fact_mobile (isp);"""
	index_cmd += """CREATE INDEX geom_gix ON olap.fact_mobile USING GIST (geom);"""
	return index_cmd
	
class MobileFact(Table):
	
	def __init__(self):
		self.table_name = 'olap.fact_mobile'
		print self.table_name
			
	def makeTable(self, sponsors):
		self.drop()
		commitCommand(self.makeTableCMD(sponsors))
		commitCommand(makeIndexesCMD())
		
	def makeTableCMD(self, sponsors):
		cmd = "CREATE TABLE " + self.table_name + " AS "
		for sponsor in sponsors[:-1]:
			for server_id in sponsor.getServerIDs():
				cmd += makeSelectCMD(server_id) + """
				UNION ALL
				"""
		for server_id in sponsors[-1].getServerIDs():
			cmd += makeSelectCMD(server_id)
		print cmd
		return cmd
