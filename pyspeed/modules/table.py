import os
from helpers import postgres

class Table(object):
			
	def drop(self):
		cmd = "DROP TABLE " + self.table_name
		print cmd
		try:
			postgres.commitCommand(cmd)
		except:
			print "TABLE DOES NOT EXIST"
		
	def createTableAs(self, select_statement):
		cmd = "CREATE TABLE " + self.table_name
		print cmd
		cmd += " AS " + select_statement
		postgres.commitCommand(cmd)
		
	def buildIndex(self, column):
		cmd = "CREATE INDEX ON "
		cmd += self.table_name + " (" + column + ")"
		print cmd
		postgres.commitCommand(cmd)
	
	def addPrimaryKey(self, column):
		cmd = "ALTER TABLE " + self.table_name
		cmd += " ADD PRIMARY KEY (" + column + ")"
		print cmd
		postgres.commitCommand(cmd)
		
	def ExportToFile(self, directory):
		cmd = 'pg_dump -F c -d speedtest --table '
		cmd += self.table_name + ' --file=' + directory
		cmd += self.table_name + '.sql'
		print cmd
		os.system(cmd)