'''
Created on 10 sept. 2016

@author: mythrys
'''
import csv

from csv_reader import UnicodeWriter, UnicodeReader


def write_csv(filename, header, data):
    file_result = open(filename, "wb")
    c = UnicodeWriter(file_result, delimiter=';', quoting=csv.QUOTE_ALL)
    c.writerow(header)
    for d in data:
        c.writerow(d)
    file_result.close()


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
