#!/usr/bin/env python
#-*- coding: utf-8 -*-
'''
Created on 27 déc. 2016

@author: Thibault Francois
'''
from lib.transform import Processor
from export_threaded import export_data
from import_threaded import import_data

class Migrator(object):

    def __init__(self, config_export, config_import):
        self.config_export = config_export
        self.config_import = config_import
        self.import_batch_size = 10
        self.import_max_con = 1
        self.export_batch_size = 100
        self.export_max_con = 1

    def migrate(self, model, domain, field_export, mappings=[None]):
        header, data = export_data(self.config_export, model, domain, field_export, max_connection=self.export_max_con, batch_size=self.export_batch_size)
        processor = Processor(header=header, data=data)
        for mapping in mappings:
            if not mapping:
                mapping = processor.get_o2o_mapping()
            to_import_header, to_import_data = processor.process(mapping, False, {})
            import_data(self.config_import, model, header=to_import_header, data=to_import_data, max_connection=self.import_max_con, batch_size=self.import_batch_size)