# -*- coding: utf-8 -*-
'''
Copyright (C) Thibault Francois

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as
published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
Lesser General Lesser Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
'''

from xmlrpclib import Fault
from time import time
from itertools import islice, chain


import sys
import csv

from lib import log_error, log_info, get_server_connection
from lib.internal.rpc_thread import RpcThread
from lib.internal.csv_reader import UnicodeWriter
from odoo_csv_tools.lib.internal.io import ListWriter

csv.field_size_limit(sys.maxint)


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
            st = time()
            try:
                self.result[batch_number] = self.model.export_data(data_ids, self.header, context=self.context)['datas']
            except Fault as e:
                log_error("export %s failed" % batch_number)
                log_error(e.faultString)
            except Exception as e:
                log_info("Unknown Problem")
                exc_type, exc_value, _ = sys.exc_info()
                # traceback.print_tb(exc_traceback, file=sys.stdout)
                log_error(exc_type)
                log_error(exc_value)
            log_info("time for batch %s: %s" % (batch_number, time() - st))

        self.spawn_thread(launch_batch_fun, [data_ids, batch_number], {})

    def write_file(self, file_writer):
        file_writer.writerow(self.header)
        for key in self.result:
            file_writer.writerows(self.result[key])


def export_data(config, model, domain, header, context=None, output=None, max_connection=1, batch_size=100, separator=';', encoding='utf-8-sig'):

    object_registry = get_server_connection(config).get_model(model)

    if output:
        file_result = open(output, "wb")
        writer = UnicodeWriter(file_result, delimiter=separator, encoding=encoding, quoting=csv.QUOTE_ALL)
    else:
        writer = ListWriter()

    rpc_thread = RPCThreadExport(int(max_connection), object_registry, header, writer, batch_size, context)
    st = time()

    ids = object_registry.search(domain, context=context)
    i = 0
    for b in batch(ids, batch_size):
        batch_ids = [l for l in b]
        rpc_thread.launch_batch(batch_ids, i)
        i += 1

    rpc_thread.wait()
    log_info("%s %s exported, total time %s second(s)" % (len(ids), model, (time() - st)))
    log_info("Writing file")
    rpc_thread.write_file(writer)
    if output:
        file_result.close()
        return False, False
    else:
        return writer.header, writer.data
