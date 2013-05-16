
__all__ = [ 'Date', 'Time', 'Timestamp', 'DateFromTicks', 'TimeFromTicks',
			'TimestampFromTicks', 'Binary', 'STRING', 'BINARY', 'NUMBER',
			'DATETIME', 'ROWID' ]

import datetime, decimal, time

class Date(object):
	
	def __init__(self, year, month, day):
		self.year 	= year
		self.month 	= month
		self.day 	= day
		
	def __str__(self):
		return "%s" % datetime.date(self.year, self.month, self.day).isoformat()

class Time(object):
	
	def __init__(self, hour, minute, second):
		self.hour 	= hour
		self.minute = minute
		self.second = second

	def __str__(self):
		return "%s" % datetime.time(self.hour, self.minute, self.second).isoformat()

class Timestamp(object):
	
	def __init__(self, year, month, day, hour, minute, second):
		self.year 	= year
		self.month 	= month
		self.day 	= day
		self.hour 	= hour
		self.minute = minute
		self.second = second
		
	def __str__(self):
		return "%s" % datetime.datetime(self.year, self.month, self.day, self.hour, self.minute, self.second).isoformat()
		
class Binary(object):
	
	def __init__(self, string):
		self.string = string
		
	def __str__(self):
		return "%s" % self.string
		
def DateFromTicks(ticks):
	return Date(*time.localtime(ticks)[:3])

def TimeFromTicks(ticks):
	return Time(*time.localtime(ticks)[3:6])

def TimestampFromTicks(ticks):
	return Timestamp(*time.localtime(ticks)[:6])

class TypeObject(object):
	def __init__(self, *values):
		self.values = values
	def __cmp__(self, other):
		if other in self.values:
			return 0
		if other < self.values:
			return 1
		return -1

print DateFromTicks(1368721972)
print TimeFromTicks(1368721972)
print TimestampFromTicks(1368721972)
print Binary(123412341234)

STRING 		= TypeObject(str)
BINARY 		= TypeObject(str)
NUMBER 		= TypeObject(int, decimal.Decimal)
DATETIME 	= TypeObject(datetime.datetime, datetime.date, datetime.time)
ROWID 		= TypeObject()
