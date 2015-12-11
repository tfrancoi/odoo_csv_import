# -*- coding: utf-8 -*-
from lib.csv_reader import UnicodeReader
from lib import conf_lib
import sys
from lib.etl_helper import *

config_file = sys.argv[1]
file_csv = conf_lib.get_file(config_file)

file_ref = open(file_csv, 'r')
reader = UnicodeReader(file_ref, delimiter=';')

head = reader.next()


basic_header = ['id', 'name']
PARTNER_PREFIX = "DEMO_PRODUCT_RES_PARTNER."
partner_header = ['id', 'name', 'is_company', 'supplier', 'customer']
partner_data = set()

SELLER_PREFIX = 'DEMO_PRODUCT_SUPPLIERINFO.'
seller_data = []
seller_header = ['id', 'name/id', 'product_tmpl_id/id', 'product_code']

TEMPLATE_PREFIX =  "DEMO_PRODUCT."
product_template_header = ['id', 'barcode', 'taxes_id', 'standard_price', 'lst_price', 'name', 'description']
product_template_line = []

default_code = set([])
i = 2
for line in reader:
    line = [s.strip() for s in line]
    line_dict = dict(zip(head, line))
    def_code = line_dict['ean13']

    pt_line = [
        TEMPLATE_PREFIX + to_xmlid(def_code),
        def_code,
        line_dict['taxes_id'],
        line_dict['standard_price'].replace(',', '.'),
        line_dict['lst_price'].replace(',', '.'),
        line_dict['name'],
        line_dict['description'],
    ]

    s_line = [
        SELLER_PREFIX + to_xmlid(def_code),
        to_m2o(PARTNER_PREFIX, line_dict['seller_ids/name']),
        TEMPLATE_PREFIX + to_xmlid(def_code),
        line_dict['seller_ids/product_code'],
    ]
    s_line +=  (len(seller_header) - len(s_line)) * ['']
    seller_data.append(s_line)

    add_m2o(partner_data, PARTNER_PREFIX, line_dict['seller_ids/name'], default=['1', '1', '0'])

    #Faster then check with element in list
    def_code_len = len(default_code)
    default_code.add(def_code)
    new_len = len(default_code)
    if def_code_len < new_len:
        product_template_line.append(pt_line)

    i += 1

write_file("data/res.partner.csv", partner_header, partner_data, worker=2, batch_size=2, init=True)
write_file("data/product.template.csv", product_template_header, product_template_line, worker=4, batch_size=5)
write_file("data/product.supplierinfo.csv", seller_header, seller_data, worker=4, batch_size=5)

