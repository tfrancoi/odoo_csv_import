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
from .lib.conf_lib import log_error, log_info, log
from .lib.internal.rpc_thread import RpcThread
from .lib.internal.io import ListWriter, open_read, open_write
from .lib.internal.csv_reader import UnicodeReader, UnicodeWriter
from .lib.internal.tools import batch

if sys.version_info >= (3, 0, 0):
    from xmlrpc.client import Fault
else:
    from xmlrpclib import Fault
    from builtins import range


class RPCThreadImport(RpcThread):

    def __init__(self, max_connection, model, header, writer, batch_size=20, context=None):
        super(RPCThreadImport, self).__init__(max_connection)
        self.model = model
        self.header = header
        self.batch_size = batch_size
        self.writer = writer
        self.context = context

    def launch_batch(self, data_lines, batch_number, check=False, o2m=False):
        def launch_batch_fun(lines, batch_number, check=False):
            i = 0
            batch_size = len(lines) if o2m else self.batch_size
            for lines_batch in batch(lines, batch_size):
                lines_batch = [l for l in lines_batch]
                self.sub_batch_run(lines_batch, batch_number, i, len(lines), check=check)
                i += 1

        self.spawn_thread(launch_batch_fun, [data_lines, batch_number], {'check': check})

    def sub_batch_run(self, lines, batch_number, sub_batch_number, total_line_nb, check=False):
        success = False

        st = time()
        try:
            success = self._send_rpc(lines, batch_number, sub_batch_number, check=check)
        except Fault as e:
            log_error("Line %s %s failed" % (batch_number, sub_batch_number))
            log_error(e.faultString)
        except ValueError as e:
            log_error("Line %s %s failed value error" % (batch_number, sub_batch_number))
        except Exception as e:
            log_info("Unknown Problem")
            exc_type, exc_value, _ = sys.exc_info()
            # traceback.print_tb(exc_traceback, file=sys.stdout)
            log_error(exc_type)
            log_error(exc_value)

        if not success:
            self.writer.writerows(lines)

        log_info("time for batch %s - %s of %s : %s" % (
        batch_number, (sub_batch_number + 1) * self.batch_size, total_line_nb, time() - st))

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
        for _ in range(1, skip):
            reader.next()

    log('open %s' % file_to_read)
    file_ref = open_read(file_to_read, encoding='utf-8-sig')
    reader = UnicodeReader(file_ref, delimiter=delimiter, encoding='utf-8-sig')
    header = next(reader)
    header = get_real_header(header)
    check_id_column(header)
    skip_line(reader)
    data = [l for l in reader]
    return header, data


"""
    Splitting helper method
"""


def split_sort(split, header, data):
    split_index = 0
    if split:
        try:
            split_index = header.index(split)
        except ValueError as ve:
            log("column %s not defined" % split)
            raise ve
        data = sorted(data, key=lambda d: d[split_index])
    return data, split_index


def do_not_split(split, previous_split_value, split_index, line, o2m=False, id_index=0):
    # Do not split if you want to keep the one2many line with it's parent
    # The column id should be empty
    if o2m and not line[id_index]:
        return True

    if not split:  # If no split no need to continue
        return False

    split_value = line[split_index]
    if split_value != previous_split_value:  # Different Value no need to not split
        return False

    return True


def import_data(config_file, model, header=None, data=None, file_csv=None, context=None, fail_file=False,
                encoding='utf-8-sig', separator=";", ignore=False, split=False, check=True, max_connection=1,
                batch_size=10, skip=0, o2m=False):
    """
        header and data mandatory in file_csv is not provided

    """
    ignore = ignore or []
    context = context or {}

    if file_csv:
        header, data = read_file(file_csv, delimiter=separator, encoding=encoding, skip=skip)
        fail_file = fail_file or file_csv + ".fail"
        file_result = open_write(fail_file, encoding=encoding)

    if not header or data == None:
        raise ValueError("Please provide either a data file or a header and data")

    object_registry = conf_lib.get_server_connection(config_file).get_model(model)

    if file_csv:
        writer = UnicodeWriter(file_result, delimiter=separator, encoding=encoding, quoting=csv.QUOTE_ALL)
    else:
        writer = ListWriter()

    writer.writerow(filter_header_ignore(ignore, header))
    if file_csv:
        file_result.flush()
    rpc_thread = RPCThreadImport(int(max_connection), object_registry, filter_header_ignore(ignore, header), writer,
                                 batch_size, context)
    st = time()

    id_index = header.index('id')
    data, split_index = split_sort(split, header, data)

    i = 0
    previous_split_value = False
    while i < len(data):
        lines = []
        j = 0
        while i < len(data) and (
                j < batch_size or do_not_split(split, previous_split_value, split_index, data[i], o2m=o2m,
                                               id_index=id_index)):
            line = data[i][:len(header)]
            lines.append(filter_line_ignore(ignore, header, line))
            previous_split_value = line[split_index]
            j += 1
            i += 1
        batch_number = split and "[%s] - [%s]" % (
        rpc_thread.thread_number(), previous_split_value) or "[%s]" % rpc_thread.thread_number()
        rpc_thread.launch_batch(lines, batch_number, check, o2m=o2m)

    rpc_thread.wait()
    if file_csv:
        file_result.close()

    log_info("%s %s imported, total time %s second(s)" % (len(data), model, (time() - st)))
    if file_csv:
        return False, False
    else:
        return writer.header, writer.data
