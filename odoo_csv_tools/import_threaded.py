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
from itertools import islice, chain
from xmlrpclib import Fault

from lib import log_error, log_info, log, get_server_connection
from lib.internal.rpc_thread import RpcThread
from lib.internal.io import ListWriter
from lib.internal.csv_reader import UnicodeReader, UnicodeWriter

csv.field_size_limit(sys.maxint)


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
    if not split:  # If no split no need to continue
        return False

    split_value = line[split_index]
    if split_value != previous_split_value:  # Different Value no need to not split
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


def read_file(fobj_read, delimiter=';', encoding='utf-8-sig', skip=0):
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

    log('open %s' % fobj_read.name)
    reader = UnicodeReader(fobj_read, delimiter=delimiter, encoding='utf-8-sig')
    header = reader.next()
    header = get_real_header(header)
    check_id_column(header)
    skip_line(reader)
    data = [l for l in reader]
    return header, data


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


def import_data(config, model, header=None, data=None, fobj_read=None, context=None, fobj_fail=False, encoding='utf-8-sig', separator=";", ignore=False, split=False, check=True, max_connection=1, batch_size=10, skip=0):
    """
        header and data mandatory in fobj_read is not provided

    """
    ignore = ignore or []
    context = context or {}

    if fobj_read:
        header, data = read_file(fobj_read, delimiter=separator, encoding=encoding, skip=skip)
        fobj_fail = fobj_fail or open(fobj_read.name + ".fail", 'wb')

    if not header or data is None:
        raise ValueError("Please provide either a data file or a header and data")

    object_registry = get_server_connection(config).get_model(model)

    if fobj_read:
        writer = UnicodeWriter(fobj_fail, delimiter=separator, encoding=encoding, quoting=csv.QUOTE_ALL)
    else:
        writer = ListWriter()

    writer.writerow(filter_header_ignore(ignore, header))
    if fobj_read:
        fobj_fail.flush()
    rpc_thread = RPCThreadImport(int(max_connection), object_registry, filter_header_ignore(ignore, header), writer, batch_size, context)
    st = time()

    data, split_index = split_sort(split, header, data)

    i = 0
    previous_split_value = False
    while i < len(data):
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
    if fobj_read:
        fobj_fail.close()

    log_info("%s %s imported, total time %s second(s)" % (len(data), model, (time() - st)))
    if fobj_read:
        return False, False
    else:
        return writer.header, writer.data
