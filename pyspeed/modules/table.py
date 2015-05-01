from helpers import postgres

tables={
	'olap.mobile_fact': 'olapfact'
	}

class Table(object):
			
	def drop(self):
		print "DROPPING TABLE"
		cmd = "DROP TABLE " + self.table_name
		postgres.commitCommand(cmd)
		
	def buildIndex(self, index):
		print "BUILDING INDEX"
		create = "CREATE INDEX ON "
		cmd = create + self.table_name + " " + index
		postgres.commitCommand(cmd)