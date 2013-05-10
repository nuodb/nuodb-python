
__all__ = [ 'Date', 'Time', 'Timestamp', 'DateFromTicks', 'TimeFromTicks',
			'TimestampFromTicks', 'Binary', 'STRING', 'BINARY', 'NUMBER',
			'DATETIME', 'ROWID' ]

import datetime, decimal, time

class Date:
	def __init__(self, year, month day):
		pass

class Time:
	def __init__(self, hour, minute, second):
		pass

class Timestamp:
	def __init__(self, year, month, day, hour, minute, second):
		pass

def DateFromTicks(ticks):
	return Date(*time.localtime(ticks)[:3])

def TimeFromTicks(ticks):
	return Time(*time.localtime(ticks)[3:6])

def TimestampFromTicks(ticks):
	return Timestamp(*time.localtime(ticks)[:6])

class Binary:
	def __init__(self, string):
		pass

class TypeObject:
	def __init__(self, *values):
		self.values = values
	def __cmp__(self, other):
		if other in self.values:
			return 0
		if other < self.values:
			return 1
		return -1

STRING = TypeObject(str)
BINARY = TypeObject(str)
NUMBER = TypeObject(int, decimal.Decimal)
DATETIME = TypeObject(datetime.datetime, datetime.date, datetime.time)
ROWID = TypeObject()
