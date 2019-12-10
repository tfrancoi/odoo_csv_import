'''
Created on 10 dec. 2019

@author: Thibault Francois
'''
import random

from odoo_csv_tools.lib import transform


processor = transform.Processor(filename='origin/test_merge1.csv')
processor.join_file("origin/test_merge2.csv", "category", "name")