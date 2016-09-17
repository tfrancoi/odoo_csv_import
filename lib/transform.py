#-*- coding: utf-8 -*-
'''
Created on 10 sept. 2016

@author: mythrys
'''
from collections import OrderedDict
from lib.internal.csv_reader import UnicodeReader
from internal.tools import ReprWrapper
from internal.file import write_file
from internal.exceptions import SkippingException

import os
import mapper

class Processor(object):
    def __init__(self, filename=None, delimiter=";", encoding='utf-8-sig', header=None, data=None):
        self.file_to_write = OrderedDict()
        if header and data:
            self.header = header
            self.data = data
        elif filename:
            self.header, self.data = self.__read_file(filename, delimiter, encoding)
        else:
            raise Exception("No Filename nor header and data provided")

    def check(self, check_fun, message=None):
        res = check_fun()
        if not res:
            if message:
                print message
            else:
                "check %s failded" % check_fun.__name__

    def split(self, split_fun):
        res = {}
        for i, d in enumerate(self.data):
            k = split_fun(dict(zip(self.header, d)), i)
            res.setdefault(k, []).append(d)
        processor_dict = {}
        for k, data in res.iteritems():
            processor_dict[k] = Processor(header=list(self.header), data=data)
        return processor_dict

    def get_o2o_mapping(self):
        """Will generate a mapping with 'key' : mapper.val('key') for each key

        you can print using pprint to print the equivalent python of the mapping to use it in your file
        
        :return: a dict where the key is a str and the value a mapper.val function, 
                 the key and the field pass to the mapper are identical
        
                {
                    'id' : mapper.val('id'),
                    .....
                }
        """
        mapping = {}
        for column in self.header:
            map_val_rep = ReprWrapper("mapper.val('%s')" %column, mapper.val(column))
            mapping[str(column)] = map_val_rep
        return mapping

    def process(self, mapping, filename_out, import_args, t='list', null_values=['NULL'], verbose=True, m2m=False):
        if m2m:
            head, data = self.__process_mapping_m2m(mapping, null_values=null_values, verbose=verbose)
        else:
            head, data = self.__process_mapping(mapping, t=t, null_values=null_values, verbose=verbose)
        import_args = dict(import_args)
        import_args['filename'] = os.path.abspath(filename_out)
        import_args['header'] = head
        import_args['data'] = data
        self.file_to_write[filename_out] = import_args

    def write_to_file(self, script_filename, fail=True, append=False, python_exe='python', path='./'):
        init = not append
        for _, info in self.file_to_write.items():
            info_copy = dict(info)
            info_copy.update({
                'model' : info.get('model', 'auto'),
                'init' : init,
                'launchfile' : script_filename,
                'fail' : fail,
                'python_exe' : python_exe,
                'path' : path,
            })

            write_file(**info_copy)
            init = False

    def get_processed_data(self, filename_out):
        return self.file_to_write[filename_out]

    ########################################
    #                                      #
    #            Private Method            #
    #                                      #
    ########################################
    def __read_file(self, filename, delimiter, encoding):
        file_ref = open(filename, 'r')
        reader = UnicodeReader(file_ref, delimiter=delimiter, encoding='utf-8-sig')
        head = reader.next()
        data = [d for d in reader]
        return head, data

    def __process_mapping(self, mapping, t, null_values, verbose):
        """
            @param t: type of return, list or set
        """
        lines_out = [] if t == 'list' else set()
        for i, line in enumerate(self.data):
            line = [s.strip() if s.strip() not in null_values else '' for s in line]
            line_dict = dict(zip(self.header, line))
            try:
                line_out = [mapping[k](line_dict) for k in mapping.keys()]
            except SkippingException as e:
                if verbose:
                    print "Skipping", i
                    print e.message
                continue
            if t == 'list':
                lines_out.append(line_out)
            else:
                lines_out.add(tuple(line_out))
    
        return mapping.keys(), lines_out

    def __process_mapping_m2m(self, mapping, null_values, verbose):
        """
            
        """
        head, data = self.__process_mapping(mapping, 'list', null_values, verbose)
        lines_out = set()
        for line_out in data:
            index_list = []
            zip_list = []
            for index, value in enumerate(line_out):
                if isinstance(value, list):
                    index_list.append(index)
                    zip_list.append(value)
            values_list = zip(*zip_list)
            for values in values_list:
                new_line = list(line_out)
                for i, val in enumerate(values):
                    new_line[index_list[i]] = val
                lines_out.add(tuple(new_line))

        return head, lines_out

    