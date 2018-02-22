#-*- coding: utf-8 -*-
'''
Created on 21 févr. 2018

@author: mythrys
'''
from odoo_csv_tools.lib import xml_transform
mapping = {
    'name' : 'year/text()',
    'gdp': 'gdppc/text()',
    'nom': '@name',
    'neighbor' : 'neighbor[1]/@name',

}

p = xml_transform.XMLProcessor("origin/data.xml", "//country", )
p.process(mapping, 'data/info.csv', { 'worker' : 2, 'batch_size' : 5})
p.write_to_file("99_contact_import.sh", python_exe='', path='')