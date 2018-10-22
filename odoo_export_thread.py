#!/usr/bin/env python
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

import argparse
from odoo_csv_tools import export_threaded

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import data in batch and in parallel')
    parser.add_argument('-c', '--config', dest='config', default="conf/connection.conf",
                        help='Configuration File that contains connection parameters', required=True)
    parser.add_argument('--file', dest='filename', help='Output File', required=True)
    parser.add_argument('--model', dest='model', help='Model to Export', required=True)
    parser.add_argument('--field', dest='fields', help='Fields to Export', required=True)
    parser.add_argument('--domain', dest='domain', help='Filter', default="[]")
    parser.add_argument('--worker', dest='worker', default=1, help='Number of simultaneous connection')
    parser.add_argument('--size', dest='batch_size', default=10, help='Number of line to import per connection')
    parser.add_argument('-s', '--sep', dest="separator", default=";", help='CSV separator')
    parser.add_argument('--context', dest='context',
                        help='context that will be passed to the load function, need to be a valid python dict',
                        default="{'tracking_disable' : True}")
    # TODO args : encoding
    # {'update_many2many': True,'tracking_disable' : True, 'create_product_variant' : True, 'check_move_validity' : False}
    args = parser.parse_args()

    config_file = args.config
    file_csv = args.filename
    batch_size = int(args.batch_size)
    model = args.model
    max_connection = int(args.worker)
    separator = args.separator
    encoding = 'utf-8-sig'
    context = eval(args.context)
    domain = eval(args.domain)
    header = args.fields.split(',')
    export_threaded.export_data(config_file, model, domain, header, context=context, output=file_csv,
                                max_connection=max_connection, batch_size=batch_size, separator=separator,
                                encoding=encoding)
