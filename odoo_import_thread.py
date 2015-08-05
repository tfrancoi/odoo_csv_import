'''
Created on 16 mai 2014

@author: openerp
'''

from csv_reader import UnicodeReader, UnicodeWriter
from xmlrpclib import Fault
import conf_lib
import sys
import threading
from time import time
from copy import deepcopy


class rpc_thread(threading.Thread):

     
    def __init__(self, semaphore, max_thread_semaphore, model, header, data_lines, model_data, writer, batch_number=0): 
        threading.Thread.__init__(self) 
        self.semaphore = semaphore 
        self.max_thread_semaphore = max_thread_semaphore
        self.model = model
        self.header = header
        self.lines = [deepcopy(l) for l in data_lines]
        
        self.model_data = model_data
        self._extract_xml_ids()

        self.writer = writer
        
        self.batch_number = batch_number
        
    def _extract_xml_ids(self):
        id_index = self.header.index('id')
        self.module_list = set()
        self.xml_ids = []
        for l in self.lines:
            xml_id = l[id_index].split('.')
            if len(xml_id) == 2:
                self.xml_ids.append(xml_id[1])
                self.module_list.add(xml_id[0])
            else:
                self.xml_ids.append(xml_id[0])
        self.module_list = list(self.module_list)
    
        
    def run(self):
        success = False
        self.semaphore.acquire()
        st = time()
        try:
            if self._send_rpc():
                success = self.check_result()
        except Fault as e:
            print "Line", i, "Failed"
            print e.faultString
        except ValueError:
            print "Line", i, "Failed"
        finally:
            self.semaphore.release()
            self.max_thread_semaphore.release()
            
        if not success:
            self.writer.writerows(self.lines)
        
        print "time for batch", self.batch_number, ":", time() - st
            
    def _send_rpc(self):
        res = {'messages' : True}
        res = self.model.load(self.header, self.lines)
        if res['messages']:
            for msg in res['messages']:
                print >> sys.stderr, msg
                print >> sys.stderr, self.lines[msg['record']]
            return False
        
        return True
        
    def check_result(self):
        domain = [['name', 'in', self.xml_ids], 
                  ['model', '=', self.model.model_name]]
        if self.module_list:
            domain.append(['module', 'in', self.module_list])
        object_ids = self.model_data.search(domain)
        return len(object_ids) == len(self.xml_ids)
            





    

config_file = sys.argv[1]
file_csv = conf_lib.get_file(config_file)
batch_size = conf_lib.get_batch_size(config_file)
model =  conf_lib.get_model(config_file)
fail_file = conf_lib.get_faile_file(config_file)
max_connection = conf_lib.get_max_connection(config_file)

if len(sys.argv) > 2 and sys.argv[2] == 'fail':
    file_csv = fail_file
    fail_file = fail_file[:-4] + "bis.csv"
    batch_size = 1
    max_connection = 1

semaphore = threading.BoundedSemaphore(int(max_connection))
max_thread_semaphore = threading.BoundedSemaphore(int(max_connection) * 10)




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
#while i < 6870:
#    reader.next()
#    i+= 1;
    
file_result = open(fail_file, "wb")

c = UnicodeWriter(file_result, delimiter=';')
c.writerow(header)
file_result.flush()
thread_list = []
st = time()
for line in reader:
    lines = [line[:header_len]]
        
    j = 1
    while j < batch_size and line:
        j += 1
        i += 1
        try:
            line = reader.next()[:header_len]
            lines.append(line)
            
        except StopIteration:
            line = False
    
    max_thread_semaphore.acquire()
    th = rpc_thread(semaphore, max_thread_semaphore, object_registry, header, lines, ir_model_registry, c, i)
    thread_list.append(th)
    th.start()
    i += 1

    
for t in thread_list:
    t.join()
file_result.close()

print "total time", time() - st
