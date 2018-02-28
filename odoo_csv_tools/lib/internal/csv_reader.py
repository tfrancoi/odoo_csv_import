'''
Created on 16 mai 2014

@author: openerp
'''
from __future__ import absolute_import
import sys
#import csv, codecs
if sys.version_info >= (3, 0, 0):
    import csv
else:
    import unicodecsv as csv
from io import StringIO
import threading

class UnicodeReader:
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        self.reader = csv.reader(f, dialect=dialect, **kwds)

    def next(self):
        #For python2
        return self.reader.next()

    def __next__(self):
        #For python3
        return self.reader.__next__()

    def __iter__(self):
        return self


class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.stream = f
        self.writer = writer = csv.writer(f, dialect=dialect, **kwds)
        self.lock = threading.RLock()

    def writerow(self, row):
        self.lock.acquire()
        self.writer.writerow(row)
        self.lock.release()

    def writerows(self, rows):
        self.lock.acquire()
        self.writer.writerows(rows)
        self.stream.flush()
        self.lock.release()
