from helpers.postgres import commitCommand

def makeSelectCMD():
	table_cmd="""SELECT
	
"""
	return table_cmd


def makeTableCMD():
	table_cmd = "CREATE TABLE olap.statedime AS "
	table_cmd += makeSelectCMD()
# 	print table_cmd
	return table_cmd

def makeIndexes():
	index_cmd = "CREATE INDEX  ON olap.statedime"
	index_cmd +=" (Year ASC NULLS LAST, Month ASC NULLS LAST, Day ASC NULLS LAST);"
	index_cmd += "CREATE INDEX  ON olap.datedim"
	index_cmd +=" (DATE DESC NULLS LAST);"
	return index_cmd

def makeTable():
	commitCommand("DROP TABLE olap.statedime")
	commitCommand(makeTableCMD())
	commitCommand(makeIndexes())
	