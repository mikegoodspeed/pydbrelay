#!/usr/local/bin/python -tt
"""Python module to connect to databases using dbrelay.
   Copyright (C) 2011  Mike Goodspeed

   This program is free software: you can redistribute it and/or modify it
   under the terms of the GNU General Public License as published by the Free
   Software Foundation, either version 3 of the License, or (at your option)
   any later version.

   This program is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
   or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
   for more details.

   You should have received a copy of the GNU General Public License along
   with this program.  If not, see <http://www.gnu.org/licenses/>.

   dbrelay: https://github.com/dbrelay/dbrelay
   DB-API: http://www.python.org/dev/peps/pep-0249/

"""

import re
import urllib
import urllib2
import decimal
import datetime
import exceptions
try: import simplejson as json
except ImportError: import json

apilevel = '2.0'
threadsafety = 1 # race conditions in Connection._params
paramstyle = 'pyformat'

class Error(exceptions.StandardError):
    pass

class Warning(exceptions.StandardError):
    pass

class InterfaceError(Error):
    pass

class DatabaseError(Error):
    pass

class InternalError(DatabaseError):
    pass

class OperationalError(DatabaseError):
    pass

class ProgrammingError(DatabaseError):
    pass

class IntegrityError(DatabaseError):
    pass

class DataError(DatabaseError):
    pass

class NotSupportedError(DatabaseError):
    pass

def Date(year, month, day):
    return None

def Time(hour, minute, second):
    return None

def Timestamp(year, month, day, hour, minute, second):
    return None

def DateFromTicks(ticks):
    return None

def TimeFromTicks(ticks):
    return None

def TimestampFromTicks(ticks):
    return None

def Binary(string):
    return None

class DBAPITypeObject(object):

    def __init__(self, *values):
        self.values = set(values)

    def __cmp__(self, other):
        if other in self.values:
            return 0
        if other < min(self.values):
            return 1
        return -1

    def to_object(self, item, type):
        return item

class DBAPIString(DBAPITypeObject):

    def __init__(self):
        DBAPITypeObject.__init__(self, 'char', 'guid', 'text', 'varchar',
                                 'wchar', 'wvarchar')

    def to_object(self, item, type):
        return str(item)

class DBAPIBinary(DBAPITypeObject):

    def __init__(self):
        DBAPITypeObject.__init__(self, 'binary', 'blob', 'enum', 'geometry',
                                 'image', 'money', 'smallmoney', 'varbinary')

    def to_object(self, item, type):
        return str(item)

class DBAPINumber(DBAPITypeObject):

    def __init__(self):
        DBAPITypeObject.__init__(self, 'bigint', 'bit', 'double', 'int',
                                 'int24', 'integer', 'float', 'longint',
                                 'longlong', 'real', 'shortint', 'tinyint',
                                 'year', 'decimal', 'numeric')

    def to_object(self, item, type):
        if type in ('decimal', 'numeric'):
            return decimal.Decimal(str(item))
        return item

class DBAPIDateTime(DBAPITypeObject):

    def __init__(self):
        DBAPITypeObject.__init__(self, 'date', 'time', 'datetime',
                                 'smalldatetime', 'timestamp')

    def to_object(self, item, type):
        if type == 'date':
            (year, month, day) = re.split('[-]', item)
            return datetime.date(int(year), int(month), int(day))
        if type == 'time':
            raw = re.split('[:.]', item)
            raw.extend([u'0'] * (4 - len(raw)))
            (hour, minute, second, micro) = raw
            return datetime.time(int(hour), int(minute), int(second),
                                 int(micro.ljust(6,'0')))
        if type in DATETIME.values:
            raw = re.split('[- :.]', item)
            raw.extend([u'0'] * (7 - len(raw)))
            (year, month, day, hour, minute, second, micro) = raw
            return datetime.datetime(int(year), int(month), int(day),
                                     int(hour), int(minute), int(second),
                                     int(micro.ljust(6,'0')))
        return item

STRING = DBAPIString()
BINARY = DBAPIBinary()
NUMBER = DBAPINumber()
DATETIME = DBAPIDateTime()
ROWID = DBAPITypeObject()

class Cursor(object):

    def __init__(self, connection):
        self.conn = connection
        self.description = None
        self.rowcount = -1
        self.arraysize = 1
        self._open = True

    def _type_map(self, item, type):
        if item is None or type == 'null':
            return None
        for dbapi_type in [STRING, BINARY, NUMBER, DATETIME, ROWID]:
            if type == dbapi_type:
                return dbapi_type.to_object(item, type)
        return item

    def close(self):
        self._open = False
        self._data = None
        self._types = None
        self.description = None

    def __del__(self):
        self.close()

    def _set_description(self):
        fields = self._data[self._resultIndex]['fields']
        self._types = [field['sql_type'] for field in fields]
        names = [field['name'] for field in fields]
        type_codes = [field['sql_type'] for field in fields]
        display_sizes = [None for field in fields]
        internal_sizes = [None for field in fields]
        precisions = [field['precision'] if 'precision' in field else None
                      for field in fields]
        scales = [field['scale'] if 'scale' in field else None
                  for field in fields]
        self.description = zip(names, type_codes, display_sizes,
                               internal_sizes, precisions, scales)

    def execute(self, operation, parameters=None):
        self._assertOpen()
        params = self.conn.params
        params['sql'] = '-- %s\n%s' % (params['connection_name'], operation)
        data = urllib.urlencode(params)
        result = json.load(urllib2.urlopen(self.conn.url, data=data))
        self._index = 0
        self._resultIndex = 0
        self._data = []
        if 'data' in result and len(result['data']) > 0:
            self._data = result['data']
            self._set_description()
            self._types = [desc[1] for desc in self.description]
        elif 'log' in result and 'error' in result['log']:
            raise DatabaseError(result['log']['error'])

    def executemany(self, operation, seq_of_parameters):
        self._assertOpen()
        for params in seq_of_parameters:
            self.execute(operation, params)

    def fetchone(self):
        self._assertOpen()
        if len(self._data) == 0 or self._data[self._resultIndex]['rows'] == 0:
            raise Error("Empty result set.")
        if self._index >= len(self._data[self._resultIndex]['rows']):
            return None
        row = self._data[self._resultIndex]['rows'][self._index]
        col_types = [row[d[0]] for d in self.description]
        row = map(self._type_map, col_types, self._types)
        self._index += 1
        return row

    def fetchmany(self, size=None):
        self._assertOpen()
        if size is None:
            size = self.arraysize
        pass # todo ... not used in getcodb

    def fetchall(self):
        self._assertOpen()
        pass # todo ... not used in getcodb

    def nextset(self):
        self._assertOpen()
        pass # possible todo ... optional

    def setinputsizes(self):
        self._assertOpen()
        pass # ignored

    def setoutputsize(self, size, column=None):
        self._assertOpen()
        pass # ignored

    def _assertOpen(self):
        if not self._open:
            raise InternalError("pydbrelay Cursor is closed.")

class Connection(object):

    def __init__(self, url, sql_server, sql_database, sql_user, sql_password,
                 connection_name, http_keepalive):
        self._open = True
        self.url = url
        self.params = {'sql_server': sql_server,
                       'sql_database': sql_database,
                       'sql_user': sql_user,
                       'sql_password': sql_password,
                       'connection_name' : connection_name,
                       'http_keepalive': http_keepalive,
                       'sql': ''}
        try:
            cur = self.cursor()
            cur.execute("select 1 as a")
            cur.fetchone()
        except Error:
            raise DatabaseError("Unable to connect to database.")
        finally:
            cur.close()

    def close(self):
        self._open = False

    def __del__(self):
        self.close()

    def commit(self):
        pass

    def cursor(self):
        if not self._open:
            raise Error("pydbrelay Connection is closed.")
        return Cursor(self)

def connect(url, sql_server, sql_database, sql_user, sql_password,
            connection_name='pydbrelay', http_keepalive=0):
    return Connection(url, sql_server, sql_database, sql_user, sql_password,
                      connection_name, http_keepalive)
