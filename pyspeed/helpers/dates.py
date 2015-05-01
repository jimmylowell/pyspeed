import datetime

def make(date_string):
	year, month, day = date_string.split('-')
	date = datetime.date(int(year), int(month), int(day))
	return date

def daterange( start_date, end_date ):
	if start_date <= end_date:
		for n in range( ( end_date - start_date ).days + 1 ):
			yield start_date + datetime.timedelta( n )
	else:
		for n in range( ( start_date - end_date ).days + 1 ):
			yield start_date - datetime.timedelta( n )