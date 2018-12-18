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

def open_read(f, encoding='utf-8-sig'):
    if not is_string(f):
        return f
    if sys.version_info >= (3, 0, 0):
        return open(f, 'r', newline='', encoding=encoding)
    else:
        return open(f, 'r')

def open_write(f, encoding='utf-8-sig'):
    if not is_string(f):
        return f
    if sys.version_info >= (3, 0, 0):
        return open(f, "w", newline='', encoding=encoding)
    else:
        return open(f, "w")

def write_csv(filename, header, data):
    file_result = open_write(filename)
    c = UnicodeWriter(file_result, delimiter=';', quoting=csv.QUOTE_ALL)
    c.writerow(header)
    for d in data:
        c.writerow(d)
    file_result.close()

def write_file(filename=None, header=None, data=None, fail=False, model="auto",
               launchfile="import_auto.sh", worker=1, batch_size=10, init=False,
               conf_file=False, groupby='', sep=";", python_exe='python', path='./', context=None, ignore=""):
    def get_model():
        if model == "auto":
            return filename.split(os.sep)[-1][:-4]
        else:
            return model

    context = '--context="%s"' % str(context) if context else ''
    conf_file = conf_file or "%s%s%s" % ('conf', os.sep, 'connection.conf')
    write_csv(filename, header, data)

    mode = init and 'w' or 'a'
    with open(launchfile, mode) as myfile:
        myfile.write("%s %sodoo_import_thread.py -c %s --file=%s --model=%s --worker=%s --size=%s --groupby=%s --ignore=%s --sep=\"%s\" %s\n" %
                    (python_exe, path, conf_file, filename, get_model(), worker, batch_size, groupby, ignore, sep, context))
        if fail:
            myfile.write("%s %sodoo_import_thread.py -c %s --fail --file=%s --model=%s --ignore=%s --sep=\"%s\" %s\n" %
                         (python_exe, path, conf_file, filename, get_model(), ignore, sep, context))


################################################
# Method to merge file together based on a key #
################################################

def write_file_dict(filename, header, data):
    data_rows = []
    for _, val in data.iteritems():
        r = [val.get(h, '') for h in header]
        data_rows.append(r)
    write_csv(filename, header, data_rows)



def read_file_dict(file_name, id_name):
    file_ref = open(file_name, 'r')
    reader = UnicodeReader(file_ref, delimiter=';')

    head = reader.next()
    res = {}
    for line in reader:
        if any(line):
            line_dict = dict(zip(head, line))
            res[line_dict[id_name]] = line_dict
    return res, head

def merge_file(master, child, field):
    res = {}
    for key, val in master.iteritems():
        data = dict(child.get(val[field], {}))
        new_dict = dict(val)
        new_dict.update(data)
        res[key] = new_dict
    return res


def merge_header(*args):
    old_header = [item for sublist in args for item in sublist]
    header = []
    for h in old_header:
        if h and h not in header:
            header.append(h)
    return header

class ListWriter(object):
    def __init__(self):
        self.data = []
        self.header = []

    def writerow(self, header):
        self.header = list(header)

    def writerows(self, line):
        self.data.extend(list(line))
