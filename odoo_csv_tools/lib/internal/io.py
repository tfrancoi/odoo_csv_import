'''
Created on 10 sept. 2016

@author: mythrys
'''
from __future__ import absolute_import

import csv
import os
import sys
from . csv_reader import UnicodeWriter, UnicodeReader

"""
    Compatibility layer between python 2.7 and python 3
"""
def is_string(f):
    if sys.version_info >= (3, 0, 0):
        return isinstance(f, str)
    else:
        return isinstance(f, basestring)

def open_read(f, encoding='utf-8'):
    if not is_string(f):
        return f
    if sys.version_info >= (3, 0, 0):
        return open(f, 'r', newline='', encoding=encoding)
    else:
        return open(f, 'r')

def open_write(f, encoding='utf-8'):
    if not is_string(f):
        return f
    if sys.version_info >= (3, 0, 0):
        return open(f, "w", newline='', encoding=encoding)
    else:
        return open(f, "w")

def write_csv(filename, header, data, encoding="utf-8"):
    file_result = open_write(filename, encoding=encoding)
    c = UnicodeWriter(file_result, delimiter=';', quoting=csv.QUOTE_ALL, encoding=encoding)
    c.writerow(header)
    for d in data:
        c.writerow(d)
    file_result.close()

def write_file(filename=None, header=None, data=None, fail=False, model="auto",
               launchfile="import_auto.sh", worker=1, batch_size=10, init=False, encoding="utf-8",
               conf_file=False, groupby='', sep=";", python_exe='', path='', context=None, ignore=""):
    def get_model():
        if model == "auto":
            return filename.split(os.sep)[-1][:-4]
        else:
            return model

    context = '--context="%s"' % str(context) if context else ''
    conf_file = conf_file or "%s%s%s" % ('conf', os.sep, 'connection.conf')
    write_csv(filename, header, data, encoding=encoding)
    if not launchfile:
        return

    mode = init and 'w' or 'a'
    with open(launchfile, mode) as myfile:
        myfile.write("%s %sodoo_import_thread.py -c %s --file=%s --model=%s --encoding=%s --worker=%s --size=%s --groupby=%s --ignore=%s --sep=\"%s\" %s\n" %
                    (python_exe, path, conf_file, filename, get_model(), encoding, worker, batch_size, groupby, ignore, sep, context))
        if fail:
            myfile.write("%s %sodoo_import_thread.py -c %s --fail --file=%s --model=%s --encoding=%s --ignore=%s --sep=\"%s\" %s\n" %
                         (python_exe, path, conf_file, filename, get_model(), encoding, ignore, sep, context))

class ListWriter(object):
    def __init__(self):
        self.data = []
        self.header = []

    def writerow(self, header):
        self.header = list(header)

    def writerows(self, line):
        self.data.extend(list(line))
