#!/usr/bin/env python
#-*- coding: utf-8 -*-
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

import argparse
from odoo_csv_tools import import_threaded

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import data in batch and in parallel')
    parser.add_argument('-c', '--config', dest='config', default="conf/connection.conf", help='Configuration File that contains connection parameters', required = True)
    parser.add_argument('--file', dest='filename', help='File to import', required = True)
    parser.add_argument('--model', dest='model', help='Model to import', required = True)
    parser.add_argument('--worker', dest='worker', default=1, help='Number of simultaneous connection')
    parser.add_argument('--size', dest='batch_size', default=10, help='Number of line to import per connection')
    parser.add_argument('--skip', dest='skip', default=0, help='Skip until line [SKIP]')
    parser.add_argument('--fail', action='store_true', dest="fail", help='Fail mode')
    parser.add_argument('-s', '--sep', dest="separator", default=";", help='CSV separator')
    parser.add_argument('--groupby', dest='split', help='Group data per batch with the same value for the given column in order to avoid concurrent update error')
    parser.add_argument('--ignore', dest='ignore', help='list of column separate by comma. Those column will be remove from the import request')
    parser.add_argument('--check', dest='check', action='store_true', help='Check if record are imported after each batch.')
    parser.add_argument('--context', dest='context', help='context that will be passed to the load function, need to be a valid python dict', default="{'tracking_disable' : True}")
    parser.add_argument('--o2m', action='store_true', dest="o2m", help="When you want to import o2m field, don't cut the batch until we find a new id")
    #TODO args : encoding
    #{'update_many2many': True,'tracking_disable' : True, 'create_product_variant' : True, 'check_move_validity' : False}
    args = parser.parse_args()

    file_csv = args.filename
    batch_size = int(args.batch_size)
    fail_file = file_csv + ".fail"
    max_connection = int(args.worker)
    split = False
    encoding='utf-8-sig'
    context= eval(args.context)
    ignore = False
    if args.ignore:
        ignore = args.ignore.split(',')

    if args.fail:
        file_csv = fail_file
        fail_file = fail_file + ".bis"
        batch_size = 1
        max_connection = 1
        split = False

    import_threaded.import_data(args.config, args.model, file_csv=file_csv, context=context,
                                fail_file=fail_file, encoding=encoding, separator=args.separator,
                                ignore=ignore, split=args.split, check=args.check,
                                max_connection=max_connection, batch_size=batch_size, skip=int(args.skip), o2m=args.o2m)
