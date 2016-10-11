#!/usr/bin/env python
#-*- coding: utf-8 -*-
'''
Created on 16 mai 2014

author: Thibault Francois <francois.th@gmail.com>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

class RPCThreadExport(RpcThread):

    def __init__(self, max_connection, model, header, writer, batch_size=20, context=None):
        super(RPCThreadExport, self).__init__(max_connection)
        self.model = model
        self.header = header
        self.batch_size = batch_size
        self.writer = writer
        self.context = context
        self.result = {}


    def launch_batch(self, data_ids, batch_number):
        def launch_batch_fun(data_ids, batch_number, check=False):
            success = False
            
            st = time()
            try:
                self.result[batch_number] = self.model.export_data(data_ids, self.header, context=self.context)['datas']
                success = True
            except Fault as e:
                log_error("export %s failed" % batch_number)
                log_error(e.faultString)
            except Exception as e:
                log_info("Unknown Problem")
                exc_type, exc_value, exc_traceback = sys.exc_info()
                #traceback.print_tb(exc_traceback, file=sys.stdout)
                log_error(exc_type)
                log_error(exc_value)
            log_info("time for batch %s: %s" % (batch_number, time() - st))
            
        self.spawn_thread(launch_batch_fun, [data_ids, batch_number], {})
        
    def write_file(self, file_writer):
        file_writer.writerow(self.header)
        for key in self.result:
            file_writer.writerows(self.result[key])
            

parser = argparse.ArgumentParser(description='Import data in batch and in parallel')
parser.add_argument('-c', '--config', dest='config', default="conf/connection.conf", help='Configuration File that contains connection parameters', required = True)
parser.add_argument('--file', dest='filename', help='Output File', required = True)
parser.add_argument('--model', dest='model', help='Model to Export', required = True)
parser.add_argument('--field', dest='fields', help='Fields to Export', required = True)
parser.add_argument('--domain', dest='domain', help='Filter', default="[]")
parser.add_argument('--worker', dest='worker', default=1, help='Number of simultaneous connection')
parser.add_argument('--size', dest='batch_size', default=10, help='Number of line to import per connection')
parser.add_argument('-s', '--sep', dest="seprator", default=";", help='CSV separator')
parser.add_argument('--context', dest='context', help='context that will be passed to the load function, need to be a valid python dict', default="{'tracking_disable' : True}")
#TODO args : encoding
#{'update_many2many': True,'tracking_disable' : True, 'create_product_variant' : True, 'check_move_validity' : False}
args = parser.parse_args()

config_file = args.config
file_csv = args.filename
batch_size = int(args.batch_size)
model = args.model
max_connection = int(args.worker)
separator = args.seprator
encoding='utf-8-sig'
context= eval(args.context)
domain = eval(args.domain)

header = args.fields.split(',')

object_registry = conf_lib.get_server_connection(config_file).get_model(model)

file_result = open(file_csv, "wb")
writer = UnicodeWriter(file_result, delimiter=separator, encoding=encoding, quoting=csv.QUOTE_ALL)

rpc_thread = RPCThreadExport(int(max_connection), object_registry, header, writer, batch_size, context)
st = time()

ids = object_registry.search(domain, context=context)

i = 0
for b in batch(ids,batch_size):
    batch_ids = [l for l in b]
    rpc_thread.launch_batch(batch_ids, i)
    i += 1

rpc_thread.wait()
log_info("%s %s imported, total time %s second(s)" % (len(ids), model, (time() - st)))
log_info("Writing file")
rpc_thread.write_file(writer)
file_result.close()
