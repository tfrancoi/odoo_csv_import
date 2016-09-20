#-*- coding: utf-8 -*-
'''
Created on 16 mai 2014

@author: Thibault Francois <francois.th@gmail.com>
'''

from lib.internal.csv_reader import UnicodeReader, UnicodeWriter
from xmlrpclib import Fault
from lib import conf_lib
from lib.conf_lib import log_error, log_info, log
import argparse
import sys
import csv
from time import time
from itertools import islice, chain
from lib.internal.rpc_thread import RpcThread

csv.field_size_limit(sys.maxsize)


def batch(iterable, size):
    sourceiter = iter(iterable)
    while True:
        batchiter = islice(sourceiter, size)
        yield chain([batchiter.next()], batchiter)

class RPCThreadImport(RpcThread):

    def __init__(self, max_connection, model, header, writer, batch_size=20, context=None):
        super(RPCThreadImport, self).__init__(max_connection)
        self.model = model
        self.header = header
        self.batch_size = batch_size
        self.writer = writer
        self.context = context


    def launch_batch(self, data_lines, batch_number, check=False):
        def launch_batch_fun(lines, batch_number, check=False):
            i = 0
            for lines_batch in batch(lines, self.batch_size):
                lines_batch = [l for l in lines_batch]
                self.sub_batch_run(lines_batch, batch_number, i, len(lines), check=check)
                i += 1

        self.spawn_thread(launch_batch_fun, [lines, batch_number], {'check' : check})

    def sub_batch_run(self, lines, batch_number, sub_batch_number, total_line_nb, check=False):
        success = False

        st = time()
        try:
            success = self._send_rpc(lines, batch_number, sub_batch_number, check=check)
        except Fault as e:
            log_error("Line %s failed" % i)
            log_error(e.faultString)
        except ValueError:
            log_error("Line %s failed" % i)
        except Exception as e:
            log_info("Unknown Problem")
            exc_type, exc_value, exc_traceback = sys.exc_info()
            #traceback.print_tb(exc_traceback, file=sys.stdout)
            log_error(exc_type)
            log_error(exc_value)

        if not success:
            self.writer.writerows(lines)

        log_info("time for batch %s - %s of %s : %s" % (batch_number, (sub_batch_number + 1) * self.batch_size, total_line_nb, time() - st))


    def _send_rpc(self, lines, batch_number, sub_batch_number, check=False):
        res = self.model.load(self.header, lines, context=self.context)
        if res['messages']:
            for msg in res['messages']:
                log_error('batch %s, %s' % (batch_number, sub_batch_number))
                log_error(msg)
                log_error(lines[msg['record']])
            return False
        if len(res['ids']) != len(lines) and check:
            log_error("number of record import is different from the record to import, probably duplicate xml_id")
            return False

        return True

def do_not_split(split, previous_split_value, split_index, line):
    if not split: # If no split no need to continue
        return False

    split_value = line[split_index]
    if split_value != previous_split_value: #Different Value no need to not split
        return False

    return True

def filter_line_ignore(ignore, header, line):
    new_line = []
    for k, val in zip(header, line):
        if k not in ignore:
            new_line.append(val)
    return new_line

def filter_header_ignore(ignore, header):
    new_header = []
    for val in header:
        if val not in ignore:
            new_header.append(val)
    return new_header

def read_file(file_to_read, delimiter=';', encoding='utf-8-sig', skip=0):
    def get_real_header(header):
        """ Get real header cut at the first empty column """
        new_header = []
        for head in header:
            if head:
                new_header.append(head)
            else:
                break
        return new_header
    
    def check_id_column(header):
        try:
            header.index('id')
        except ValueError as ve:
            log_error("No External Id (id) column defined, please add one")
            raise ve
    
    def skip_line(reader):
        log_info("Skipping until line %s excluded" % skip)
        for _ in xrange(1, skip):
            reader.next()

    log('open %s' % file_csv)
    file_ref = open(file_csv, 'r')
    reader = UnicodeReader(file_ref, delimiter=separator, encoding='utf-8-sig')
    header = reader.next()
    header = get_real_header(header)
    check_id_column(header)
    skip_line(reader)
    data = [l for l in reader]
    return header, data

def split_sort(split, data):
    split_index = 0
    if split:
        try:
            split_index = header.index(split)
        except ValueError as ve:
            log("column %s not defined" % split)
            raise ve
        data = sorted(data, key=lambda d: d[split_index])
    return data, split_index

parser = argparse.ArgumentParser(description='Import data in batch and in parallel')
parser.add_argument('-c', '--config', dest='config', default="conf/connection.conf", help='Configuration File', required = True)
parser.add_argument('--file', dest='filename', help='File to import', required = True)
parser.add_argument('--model', dest='model', help='Model to import, if auto try to guess the model from the filename', required = True)
parser.add_argument('--worker', dest='worker', default=1, help='Number of simultaneous connection')
parser.add_argument('--size', dest='batch_size', default=10, help='Number of line to import per connection')
parser.add_argument('--skip', dest='skip', default=0, help='Skip until line [SKIP]')
parser.add_argument('-f', '--fail',action='store_true', dest="fail", help='Fail mode')
parser.add_argument('-s', '--sep', dest="seprator", default=";", help='Fail mode')
parser.add_argument('--groupby', dest='split', help='Group data per batch with the same value for the given column in order to avoid concurrent update error')
parser.add_argument('--ignore', dest='ignore', help='Keep batch same value of the field in the same batch')
parser.add_argument('--check', dest='check', action='store_true', help='Check if record are imported after each batch. Can slow down the process')
parser.add_argument('--context', dest='context', help='context that will be passed to the load function, need to be a valid python dict', default="{'tracking_disable' : True}")
#TODO args : encoding
#{'update_many2many': True,'tracking_disable' : True, 'create_product_variant' : True, 'check_move_validity' : False}
args = parser.parse_args()

config_file = args.config
file_csv = args.filename
batch_size = int(args.batch_size)
model = args.model
fail_file = file_csv + ".fail"
max_connection = int(args.worker)
separator = args.seprator
split = False
check = args.check
encoding='utf-8-sig'
context= eval(args.context)

if args.split:
    split = args.split
    
if args.fail:
    file_csv = fail_file
    fail_file = fail_file + ".bis"
    batch_size = 1
    max_connection = 1
    split = False
    
ignore = []
if args.ignore:
    ignore = args.ignore.split(',')

header, data = read_file(file_csv, delimiter=separator, encoding=encoding, skip=int(args.skip))

object_registry = conf_lib.get_server_connection(config_file).get_model(model)

file_result = open(fail_file, "wb")

writer = UnicodeWriter(file_result, delimiter=separator, encoding=encoding, quoting=csv.QUOTE_ALL)
writer.writerow(filter_header_ignore(ignore, header))
file_result.flush()
rpc_thread = RPCThreadImport(int(max_connection), object_registry, filter_header_ignore(ignore, header), writer, batch_size, context)
st = time()


data, split_index = split_sort(split, data)

i = 0
previous_split_value = False
while  i < len(data):
    lines = []
    j = 0
    while i < len(data) and (j < batch_size or do_not_split(split, previous_split_value, split_index, data[i])):
        line = data[i][:len(header)]
        lines.append(filter_line_ignore(ignore, header, line))
        previous_split_value = line[split_index]
        j += 1
        i += 1
    batch_number = split and "[%s] - [%s]" % (rpc_thread.thread_number(), previous_split_value) or "[%s]" % rpc_thread.thread_number()
    rpc_thread.launch_batch(lines, batch_number, check)

rpc_thread.wait()
file_result.close()

log_info("%s %s imported : total time %s second(s)" % (len(data), model, (time() - st)))
