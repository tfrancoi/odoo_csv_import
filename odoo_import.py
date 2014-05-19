'''
Created on 16 mai 2014

@author: openerp
'''

from csv_reader import UnicodeReader, UnicodeWriter
from xmlrpclib import Fault
import conf_lib
import sys
from time import time

config_file = sys.argv[1]
file_csv = conf_lib.get_file(config_file)
batch_size = conf_lib.get_batch_size(config_file)
model =  conf_lib.get_model(config_file)
fail_file = conf_lib.get_faile_file(config_file)
file_ref = open(file_csv, 'r')
reader = UnicodeReader(file_ref)

connection = conf_lib.get_server_connection(config_file)
object_registry = connection.get_model(model)
ir_model_registry = connection.get_model('ir.model.data')

header = reader.next()
try:
    id_index = header.index('id')
except ValueError as ve:
    print "No External Id (id) column defined, please add one"
    raise ve
    
i = 1
file_result = open(fail_file, "wb")

c = UnicodeWriter(file_result)
c.writerow(header)
file_result.flush()
for line in reader:
    st = time()
    lines = [line]
    xml_ids = [line[id_index]]
    success = False
    j = 1
    while j < batch_size:
        j += 1
        i += 1
        line = reader.next()
        lines.append(line)
        xml_ids.append(line[id_index])
        
    try:
        object_registry.load(header, lines)
        object_ids = ir_model_registry.search([['name', 'in', xml_ids], ['model', '=', model]])
        if len(object_ids) == len(xml_ids):
            success = True
    except Fault as e:
        #import pdb; pdb.set_trace()
        print "Line", i, "Failed"
        print e.faultString
    except ValueError as ve:
        print "Line", i, "Failed"
        
    if not success:
        c.writerows(lines)
        file_result.flush()
    print "progress", i
    i += 1
    print "time per batch", time() - st
file_result.close()
