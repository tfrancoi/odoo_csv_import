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
import sys
import csv

from time import time

from .lib import conf_lib
from .lib.conf_lib import log_error, log_info
from .lib.internal.rpc_thread import RpcThread
from .lib.internal.csv_reader import UnicodeWriter
from .lib.internal.io import ListWriter, open_write
from .lib.internal.tools import batch

if sys.version_info >= (3, 0, 0):
    from xmlrpc.client import Fault

    csv.field_size_limit(sys.maxsize)
else:
    from xmlrpclib import Fault

    csv.field_size_limit(sys.maxint)


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


def export_data(config_file, model, domain, header, context=None, output=None, max_connection=1, batch_size=100,
                separator=';', encoding='utf-8-sig'):
    object_registry = conf_lib.get_server_connection(config_file).get_model(model)

    if output:
        file_result = open_write(output)
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
