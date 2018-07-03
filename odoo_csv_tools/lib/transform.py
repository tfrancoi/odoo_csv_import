#-*- coding: utf-8 -*-
'''
Created on 10 sept. 2016

@author: mythrys
'''
import os

from collections import OrderedDict

from . internal.csv_reader import UnicodeReader
from . internal.tools import ReprWrapper, AttributeLineDict
from . internal.io import write_file, is_string, open_read
from . internal.exceptions import SkippingException
from . import mapper


class Processor(object):
    def __init__(self, filename=None, delimiter=";", encoding='utf-8-sig', header=None, data=None, preprocess=lambda header, data: (header, data), conf_file=False):
        self.file_to_write = OrderedDict()
        if header and data:
            self.header = header
            self.data = data
        elif filename:
            self.header, self.data = self.__read_file(filename, delimiter, encoding)
        else:
            raise Exception("No Filename nor header and data provided")
        self.header, self.data = preprocess(self.header, self.data)
        self.conf_file = conf_file

    def check(self, check_fun, message=None):
        res = check_fun(self.header, self.data)
        if not res:
            if message:
                print(message)
            else:
                print("%s failed" % check_fun.__name__)
        return res

    def split(self, split_fun):
        res = {}
        for i, d in enumerate(self.data):
            k = split_fun(dict(zip(self.header, d)), i)
            res.setdefault(k, []).append(d)
        processor_dict = {}
        for k, data in res.items():
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

    def process(self, mapping, filename_out, import_args, t='list', null_values=['NULL', False], verbose=True, m2m=False):
        if m2m:
            head, data = self.__process_mapping_m2m(mapping, null_values=null_values, verbose=verbose)
        else:
            head, data = self.__process_mapping(mapping, t=t, null_values=null_values, verbose=verbose)
        self._add_data(head, data, filename_out, import_args)
        return head, data

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
                'conf_file' : self.conf_file,
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
        file_ref = open_read(filename, encoding=encoding)
        reader = UnicodeReader(file_ref, delimiter=delimiter, encoding=encoding)
        head = next(reader)
        data = [d for d in reader]
        return head, data

    def __process_mapping(self, mapping, t, null_values, verbose):
        """
            @param t: type of return, list or set
        """
        lines_out = [] if t == 'list' else set()
        for i, line in enumerate(self.data):
            line = [s.strip() if s and s.strip() not in null_values else '' for s in line]
            line_dict = dict(zip(self.header, line))
            try:
                line_out = [mapping[k](line_dict) for k in mapping.keys()]
            except SkippingException as e:
                if verbose:
                    print("Skipping", i)
                    print(e.message)
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

    def _add_data(self, head, data, filename_out, import_args):
        import_args = dict(import_args)
        import_args['filename'] = os.path.abspath(filename_out) if filename_out else False
        import_args['header'] = head
        import_args['data'] = data
        self.file_to_write[filename_out] = import_args


class ProductProcessorV9(Processor):
    def __generate_attribute_data(self, attributes_list, ATTRIBUTE_PREFIX):
            self.attr_header = ['id', 'name']
            self.attr_data = [[mapper.to_m2o(ATTRIBUTE_PREFIX, att), att] for att in attributes_list]

    def process_attribute_mapping(self, mapping, line_mapping, attributes_list, ATTRIBUTE_PREFIX, path, import_args, id_gen_fun=None, null_values=['NULL']):
        """
            Mapping : name is mandatory vat_att(attribute_list)
        """
        def add_value_line(values_out, line):
            for att in attributes_list:
                value_name = line[list(mapping.keys()).index('name')].get(att)
                if value_name:
                    line_value = [ele[att] if isinstance(ele, dict) else ele for ele in line]
                    values_out.add(tuple(line_value))

        id_gen_fun = id_gen_fun or (lambda template_id, values : mapper.to_m2o(template_id.split('.')[0] + '_LINE', template_id))

        values_header = mapping.keys()
        values_data = set()

        self.__generate_attribute_data(attributes_list, ATTRIBUTE_PREFIX)
        att_data = AttributeLineDict(self.attr_data, id_gen_fun)
        for line in self.data:
            line = [s.strip() if s.strip() not in null_values else '' for s in line]
            line_dict = dict(zip(self.header, line))
            line_out = [mapping[k](line_dict) for k in mapping.keys()]

            add_value_line(values_data, line_out)
            values_lines = [line_mapping[k](line_dict) for k in line_mapping.keys()]
            att_data.add_line(values_lines, line_mapping.keys())

        line_header, line_data = att_data.generate_line()
        context = import_args.get('context', {})
        context['create_product_variant'] = True
        import_args['context'] = context
        self._add_data(self.attr_header, self.attr_data, path + 'product.attribute.csv', import_args)
        self._add_data(values_header, values_data, path + 'product.attribute.value.csv', import_args)
        import_args = dict(import_args, groupby='product_tmpl_id/id')
        self._add_data(line_header, line_data, path + 'product.attribute.line.csv', import_args)

class ProductProcessorV10(Processor):
    def process_attribute_data(self, attributes_list, ATTRIBUTE_PREFIX, filename_out, import_args):
        attr_header = ['id', 'name']
        attr_data = [[mapper.to_m2o(ATTRIBUTE_PREFIX, att), att] for att in attributes_list]
        self._add_data(attr_header, attr_data, filename_out, import_args)
