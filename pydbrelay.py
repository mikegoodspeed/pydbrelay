#!/usr/bin/env python
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
version = '0.7.0'

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
            (yr, mo, day) = map(int, re.split('[-]', item))
            return datetime.date(yr, mo, day)
        if type == 'time':
            raw = re.split('[:.]', item)
            raw.extend([u'0'] * (4 - len(raw)))
            (hour, minute, second, micro) = map(int, raw)
            micro = int(str(micro).ljust(6, '0'))
            return datetime.time(hour, minute, second, micro)
        if type in DATETIME.values:
            raw = re.split('[- :.]', item)
            raw.extend([u'0'] * (7 - len(raw)))
            (yr, mo, day, hour, minute, second, micro) = map(int, raw)
            micro = int(str(micro).ljust(6, '0'))
            return datetime.datetime(yr, mo, day, hour, minute, second, micro)
        return item

STRING = DBAPIString()
BINARY = DBAPIBinary()
NUMBER = DBAPINumber()
DATETIME = DBAPIDateTime()
ROWID = DBAPITypeObject()

class DBRelayData(object):

    def __init__(self, data):
        if not data:
            raise ValueError("No data!")
        self._json = data

    @property
    def error(self):
        log = self._json['log']
        return log['error'] if log and 'error' in log else None

    @property
    def data(self):
        return self._json['data'] if 'data' in self._json else None

    def get_fields(self, idx):
        if self.data and 0 <= idx and idx < len(self.data):
            return self.data[idx]['fields']
        return None

    def get_sql_types(self, idx):
        fields = self.get_fields(idx)
        return [field['sql_type'] for field in fields] if fields else []

    def get_names(self, idx):
        fields = self.get_fields(idx)
        return [field['name'] for field in fields] if fields else[]

    def get_precisions(self, idx):
        fields = self.get_fields(idx)
        result = []
        for field in fields:
            if 'precision' in field:
                result.append(field['precision'])
            else:
                result.append(None)
        return result

    def get_scales(self, idx):
        fields = self.get_fields(idx)
        result = []
        for field in fields:
            if 'scale' in field:
                result.append(field['scale'])
            else:
                result.append(None)
        return result

    def get_blanks(self, idx):
        fields = self.get_fields(idx)
        return [None for field in fields] if fields else []

    def get_rows(self, idx):
        if self.data and 0 <= idx and idx < len(self.data):
            return self.data[idx]['rows']
        return None

class Cursor(object):

    def __init__(self, connection):
        self.conn = connection
        self.description = None
        self.rowcount = -1
        self.arraysize = 1
        self._open = True
        self._data = None
        self._set_idx = -1
        self._row_idx = -1

    def _type_map(self, element, sql_type):
        if element is None or sql_type == 'null':
            return None
        for dbapi_type in [NUMBER, STRING, DATETIME, BINARY, ROWID]:
            if sql_type == dbapi_type:
                return dbapi_type.to_object(element, sql_type)
        return element

    def close(self):
        self._open = False
        self._data = None
        self._types = None
        self.description = None

    def __del__(self):
        self.close()

    def _set_description(self, idx):
        data = self._data
        names = data.get_names(idx)
        type_codes = data.get_blanks(idx) # fix me
        display_sizes = data.get_blanks(idx)
        internal_size = data.get_blanks(idx)
        precisions = data.get_precisions(idx)
        scales = data.get_scales(idx)
        self.description = zip(names, type_codes, display_sizes,
                               internal_size, precisions, scales)

    def execute(self, operation, parameters=None):
        self._assertOpen()
        params = self.conn.params
        params['sql'] = '-- %s\n%s' % (params['connection_name'], operation)
        data = urllib.urlencode(params)
        results = json.load(urllib2.urlopen(self.conn.url, data=data))
        self._data = DBRelayData(results)
        if self._data.error:
            raise DatabaseError(self._data.error)
        self._set_idx = -1
        self.nextset()

    def executemany(self, operation, seq_of_parameters):
        self._assertOpen()
        for params in seq_of_parameters:
            self.execute(operation, params)

    def fetchone(self):
        self._assertOpen()
        if not self._data:
            raise Error("No result set.")
        data = self._data.data
        if not data or len(data) == 0:
            raise Error("Empty result set.")
        if self._set_idx >= len(data):
            return None
        rows = self._data.get_rows(self._set_idx)
        if self._row_idx >= len(rows):
            return None
        row = rows[self._row_idx]
        cols = self._data.get_names(self._set_idx)
        elements = [row[col] for col in cols]
        sql_types = self._data.get_sql_types(self._set_idx)
        row = map(self._type_map, elements, sql_types)
        self._row_idx += 1
        return row

    def fetchmany(self, size=None):
        self._assertOpen()
        if size is None:
            size = self.arraysize
        result = []
        for i in xrange(size):
            row = self.fetchone()
            if not row:
                break
            result.append(self.fetchone())
        return result

    def fetchall(self):
        self._assertOpen()
        result = []
        row = self.fetchone()
        while row:
            result.append(row)
            row = self.fetchone()
        return result

    def nextset(self):
        self._assertOpen()
        if not self._data:
            return None
        data = self._data.data
        if not data or self._set_idx >= len(data) - 1:
            return None
        self._set_idx += 1
        self._row_idx = 0
        self._set_description(self._set_idx)
        return True

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
