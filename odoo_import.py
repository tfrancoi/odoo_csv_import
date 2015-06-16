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
reader = UnicodeReader(file_ref, delimiter=';')
print 'open', file_csv


connection = conf_lib.get_server_connection(config_file)
object_registry = connection.get_model(model)
ir_model_registry = connection.get_model('ir.model.data')

header = reader.next()
header_len = 0
for head in header:
    if head:
        header_len += 1
    else:
        break
        
header = header[:header_len]
        
try:
    id_index = header.index('id')
except ValueError as ve:
    print "No External Id (id) column defined, please add one"
    raise ve
    
i = 1
file_result = open(fail_file, "wb")

c = UnicodeWriter(file_result, delimiter=';')
c.writerow(header)
file_result.flush()
for line in reader:
    st = time()
    xml_id = line[id_index].split('.')
    if len(xml_id) == 2:
        module_list = [xml_id[0]]
        xml_ids = [xml_id[1]]
    else:
        xml_ids = [xml_id[0]]
        module_list = []

    lines = [line[:header_len]]
        
    success = False
    j = 1
    while j < batch_size and line:
        j += 1
        i += 1
        try:
            line = reader.next()[:header_len]
            lines.append(line)
            xml_id = line[id_index].split('.')
            if len(xml_id) == 2:
                xml_ids.append(xml_id[1])
                module_list.append(xml_id[0])
            else:
                xml_ids.append(xml_id[0])

            
        except StopIteration:
            line = False

    try:
        res = object_registry.load(header, lines)
        print res
        if res['messages']:
            for msg in res['messages']:
                print msg
                print lines[msg['record']]
                print "------------------------"
        module_list = list(set(module_list))
        object_ids = ir_model_registry.search([['name', 'in', xml_ids], ['module', 'in', module_list],['model', '=', model]])
        print 'ids', len(object_ids), 'xml_ids', len(xml_ids)
        print 'module', module_list
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
