'''
Created on 16 mai 2014

@author: openerp
'''

from csv_reader import UnicodeReader, UnicodeWriter
from xmlrpclib import Fault
from conf_lib import *
import sys
import threading
from time import time
from copy import deepcopy

from itertools import islice, chain

def batch(iterable, size):
    sourceiter = iter(iterable)
    while True:
        batchiter = islice(sourceiter, size)
        yield chain([batchiter.next()], batchiter)
        
class rpc_thread(threading.Thread):

     
    def __init__(self, semaphore, max_thread_semaphore, model, header, ids, data, batch_number=0): 
        threading.Thread.__init__(self) 
        self.semaphore = semaphore 
        self.max_thread_semaphore = max_thread_semaphore
        self.model = model
        self.header = header
        self.data = data
        self.ids = [i for i in ids]
        
        self.batch_number = batch_number
        
    def run(self):
        success = False
        self.semaphore.acquire()
        st = time()
        try:
            res = self._send_rpc()
            self.data.extend(res['datas'])
            
        except Fault as e:
            print "Line", "Failed"
            print e.faultString
        except ValueError:
            print "Line", "Failed"
        finally:
            self.semaphore.release()
            self.max_thread_semaphore.release()
            
        print "time for batch", self.batch_number, ":", time() - st
            
    def _send_rpc(self):
        return self.model.export_data(self.ids, self.header)
        






batch_size = 200
max_connection = 1


semaphore = threading.BoundedSemaphore(max_connection)
max_thread_semaphore = threading.BoundedSemaphore(20)

connection = get_server_connection("connection_openerp.conf")
model = connection.get_model('account.move.line')
ids = model.search([])

header = [ 'name',
'account_id/code',
'account_id/name',
'result',
'balance',
'credit',
'debit',
'date',
'invoice/number',
'journal_id/code',
'journal_id/name',
'move_id/name',
'partner_id/name',
'period_id/name',
'ref',
'tax_code_id/name',
]
  
file_header = ['Klantnummer', 'Naam klant', 'Branche', 'Opt-out', 'Straat', 'Huisnummer', 
 'Postcode', 'Plaats', 'Land' , 'Straat Postal', 'Huisnummer Postal',
'Postcode Postal',  'Plaats Postal',
'Land Postal' , 'Telefoon', 'Mobiel', 'Voornaam achternaam', 'Aanhef', 
'Titel voor', 'Voorletters', 'Voornaam', 'Tussenvoegsel', 'Achternaam' , 'Titel Achter', 
'Telefoon', 'Mobiele telefoon', 'Ondernemend', 'Briefaanhef', 'Oud Verkoopkantoor SAP' ]

file_header = header

import pprint
#pprint.pprint(dict(zip(header, file_header)))

def export(ids, file_name, header, model):

    i = 1
    
    file_result = open(file_name, "wb")
    c = UnicodeWriter(file_result, delimiter=';')
    c.writerow(file_header)

    data = []
    thread_list = []
    st = time()
    for lines in batch(ids, batch_size):
        #print len([l for l in lines])
        
        max_thread_semaphore.acquire()
        th = rpc_thread(semaphore, max_thread_semaphore, model, header, lines, data, i)
        thread_list.append(th)
        th.start()
        i += 1

        
    for t in thread_list:
        t.join()
        
    print "total time", time() - st
    print "Filter data"
    data = [d for d in data if d[1]]
    print len(file_header)
    print len(data[0])
    print "write file"
    c.writerows(data)
    print "total time writing file", time() - st
    
    file_result.close()

export(ids, 'move_line.csv', header, model)
#export(agro_ids, 'agro_export.csv')

