"""A module for housing the datatype classes.

Exported Classes:
Binary -- Class for a Binary object

Exported Functions:
DateFromTicks -- Converts ticks to a Date object.
TimeFromTicks -- Converts ticks to a Time object.
TimestampFromTicks -- Converts ticks to a Timestamp object.
DateToTicks -- Converts a Date object to ticks.
TimeToTicks -- Converts a Time object to ticks.
TimestampToTicks -- Converts a Timestamp object to ticks.
TypeObjectFromNuodb -- Converts a Nuodb column type name to a TypeObject variable.

TypeObject Variables:
STRING -- TypeObject(str)
BINARY -- TypeObject(str)
NUMBER -- TypeObject(int, decimal.Decimal)
DATETIME -- TypeObject(datetime.datetime, datetime.date, datetime.time)
ROWID -- TypeObject()
"""

__all__ = [ 'Date', 'Time', 'Timestamp', 'DateFromTicks', 'TimeFromTicks',
            'TimestampFromTicks', 'DateToTicks', 'TimeToTicks', 'TimestampToTicks',
            'Binary', 'STRING', 'BINARY', 'NUMBER', 'DATETIME', 'ROWID', 'TypeObjectFromNuodb' ]

from datetime import datetime as Timestamp, date as Date, time as Time, timedelta as TimeDelta
import decimal, time
from exception import DataError

class Binary(object):
    
    """Class for a Binary object.
    
    Private Functions:
    __init__ -- Constructor for the Binary class.
    __str__ -- Stringifies the Binary object.
    __eq__ -- Checks equality of two Binary objects.
    """
    
    def __init__(self, string):
        """Constructor for the Binary class."""
        self.string = string
        
    def __str__(self):
        """Stringifies the Binary object."""
        return self.string
    
    def __eq__(self, other):
        """Checks equality of two Binary objects."""
        if isinstance(other, Binary):
            return self.string == other.string
        else:
            return False
        
def DateFromTicks(ticks):
    """Converts ticks to a Date object."""
    return Date(*time.localtime(ticks)[:3])

def TimeFromTicks(ticks, micro = 0):
    """Converts ticks to a Time object."""
    return Time(*time.localtime(ticks)[3:6] + (micro,))

def TimestampFromTicks(ticks, micro = 0):
    """Converts ticks to a Timestamp object."""
    return Timestamp(*time.localtime(ticks)[:6] + (micro,))

def DateToTicks(value):
    """Converts a Date object to ticks."""
    timeStruct = Date(value.year, value.month, value.day).timetuple()
    return int(time.mktime(timeStruct))

def TimeToTicks(value):
    """Converts a Time object to ticks."""
    timeStruct = TimeDelta(hours = value.hour, minutes = value.minute, seconds = value.second, microseconds = value.microsecond)
    timeDec = decimal.Decimal(str(timeStruct.total_seconds()))
    return (int((timeDec + time.timezone) * 10**abs(timeDec.as_tuple()[2])), abs(timeDec.as_tuple()[2]))

def TimestampToTicks(value):
    """Converts a Timestamp object to ticks."""
    timeStruct = Timestamp(value.year, value.month, value.day, value.hour, value.minute, value.second).timetuple()
    if value.microsecond:
        micro = decimal.Decimal(value.microsecond) / decimal.Decimal(1000000)
        return (int((decimal.Decimal(int(time.mktime(timeStruct))) + micro) * decimal.Decimal(int(10**(len(str(micro)) - 2)))), len(str(micro)) - 2)
    else:
        return (int(time.mktime(timeStruct)), 0)

class TypeObject(object):
    def __init__(self, *values):
        self.values = values
    def __cmp__(self, other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        return -1

STRING         = TypeObject(str)
BINARY         = TypeObject(str)
NUMBER         = TypeObject(int, decimal.Decimal)
DATETIME     = TypeObject(Timestamp, Date, Time)
ROWID         = TypeObject()

def TypeObjectFromNuodb(nuodb_type_name):
    """Returns one of STRING, BINARY, NUMBER, DATETIME, ROWID based on the 
    supplied NuoDB column type name
    """
    
    if nuodb_type_name == "<null>":
        return None
        
    elif nuodb_type_name == "string":
        return STRING
        
    elif nuodb_type_name == "char":
        return STRING
        
    elif nuodb_type_name == "varchar":
        return STRING
        
    elif nuodb_type_name == "smallint":
        return NUMBER
        
    elif nuodb_type_name == "integer":
        return NUMBER
        
    elif nuodb_type_name == "bigint":
        return NUMBER
        
    elif nuodb_type_name == "float":
        return NUMBER
        
    elif nuodb_type_name == "double":
        return NUMBER
        
    elif nuodb_type_name == "date":
        return DATETIME
        
    elif nuodb_type_name == "timestamp":
        return DATETIME
        
    elif nuodb_type_name == "time":
        return DATETIME
        
    elif nuodb_type_name == "clob":
        return BINARY
        
    elif nuodb_type_name == "blob":
        return BINARY
        
    elif nuodb_type_name == "numeric":
        return NUMBER
        
    elif nuodb_type_name == "number":
        return NUMBER
        
    elif nuodb_type_name == "bytes":
        return BINARY
        
    elif nuodb_type_name == "binarystring":
        return BINARY
        
    elif nuodb_type_name == "binaryvaryingstring":
        return BINARY
        
    elif nuodb_type_name == "boolean":
        #TODO: Not sure about this?
        return NUMBER

    else:
        raise DataError('received unknown column type from the database')

