from helpers.postgres import commitCommand

def makeSelectCMD():
	table_cmd="""SELECT
	datum AS Date,
	extract(year FROM datum) AS Year,
	extract(month FROM datum) AS Month,
	-- Localized month name
	to_char(datum, 'TMMonth') AS MonthName,
	extract(day FROM datum) AS Day,
	extract(doy FROM datum) AS DayOfYear,
	-- Localized weekday
	to_char(datum, 'TMDay') AS WeekdayName,
	-- ISO calendar week
	extract(week FROM datum) AS CalendarWeek,
	'Q' || to_char(datum, 'Q') AS Quartal,
	to_char(datum, 'yyyy/"Q"Q') AS YearQuartal,
	to_char(datum, 'yyyy/mm') AS YearMonth,
	-- ISO calendar year and week
	to_char(datum, 'iyyy/IW') AS YearCalendarWeek,
	-- Weekend
	CASE WHEN extract(isodow FROM datum) IN (6, 7) THEN 'Weekend' ELSE 'Weekday' END AS Weekend
FROM (
	-- There are 3 leap years in this range, so calculate 365 * 10 + 3 records
	SELECT '2010-01-01'::DATE + sequence.day AS datum
	FROM generate_series(0,3652) AS sequence(day)
	GROUP BY sequence.day
     ) DQ
ORDER BY 1
"""
	return table_cmd


def makeTableCMD():
	table_cmd = "CREATE TABLE olap.datedim AS "
	table_cmd += makeSelectCMD()
# 	print table_cmd
	return table_cmd

def makeIndexes():
	index_cmd = "CREATE INDEX  ON olap.datedim"
	index_cmd +=" (Year ASC NULLS LAST, Month ASC NULLS LAST, Day ASC NULLS LAST);"
	index_cmd += "CREATE INDEX  ON olap.datedim"
	index_cmd +=" (DATE DESC NULLS LAST);"
	return index_cmd

def makeTable():
	self.drop()
	commitCommand(makeTableCMD())
	commitCommand(makeIndexes())
	