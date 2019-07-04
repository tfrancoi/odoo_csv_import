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
import os
from odoo_csv_tools.lib import mapper
from odoo_csv_tools.lib.transform import Processor

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert csv column Image Path into base64')
    parser.add_argument('file', metavar='F', help='file to convert')
    parser.add_argument('--path', dest='path', help='Image Path Prefix, default is the working directory')
    parser.add_argument('--out', dest='out', help='name of the result file, default out.csv', default="out.csv")
    parser.add_argument('-f', dest='fields', help='Fields to convert from path to base64, comma separated', required = True)
    args = parser.parse_args()

    file_csv = args.file
    out_csv  = args.out
    path = args.path
    fields = args.fields
    if not path:
        path = os.getcwd()
    if not path.endswith(os.sep):
        path += os.sep


    processor = Processor(file_csv)
    mapping = processor.get_o2o_mapping()
    for f in fields.split(','):
        f = f.strip()
        mapping[f] = mapper.binary_map(mapper.remove_sep_mapper(f), path)
    processor.process(mapping, out_csv, {}, 'list')
    processor.write_to_file("")

