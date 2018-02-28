'''
Created on 9 sept. 2016

@author: Thibault Francois <francois.th@gmail.com>
'''
from itertools import islice, chain

def batch(iterable, size):
    sourceiter = iter(iterable)
    while True:
        batchiter = islice(sourceiter, size)
        yield chain([next(batchiter)], batchiter)
"""
    Data formatting tools
"""
def to_xmlid(name):
    return name.replace('.', '_').replace(',', '_').replace('\n', '_').strip()

def list_to_xml_id(names):
    return '_'.join([to_xmlid(name) for name in names])

def to_m2o(PREFIX, value, default=''):
    if not value:
        return default
    return PREFIX + '.' + to_xmlid(value)

def to_m2m(PREFIX, value):
    if not value:
        return ''

    ids = []
    for val in value.split(','):
        if val.strip():
            ids.append(PREFIX + '.' + to_xmlid(val))
    return ','.join(ids)

def generate_attribute_list(PREFIX, *attributes):
    header = ['id', 'name']
    lines = set()
    for att in attributes:
        lines.add((to_m2o(PREFIX, att), att))
    return header, lines

"""
    Secondary data file helper

"""
class ReprWrapper(object):
    def __init__(self, repr_str, func):
        self._repr = repr_str
        self._func = func

    def __call__(self, *args, **kw):
        return self._func(*args, **kw)

    def __repr__(self):
        return self._repr

class AttributeLineDict:
    def __init__(self, attribute_list_ids, id_gen_fun):
        self.data = {}
        self.att_list = attribute_list_ids
        self.id_gen = id_gen_fun

    def add_line(self, line, header):
        """
            line = ['product_tmpl_id/id' : id, 'attribute_id/id' : dict (att : id), 'value_ids/id' : dict(att: id)]
        """
        line_dict = dict(zip(header, line))
        if self.data.get(line_dict['product_tmpl_id/id']):
            for att_id, att in self.att_list:
                if not line_dict['attribute_id/id'].get(att):
                    continue
                template_info = self.data[line_dict['product_tmpl_id/id']]
                template_info.setdefault(att_id, [line_dict['value_ids/id'][att]]).append(line_dict['value_ids/id'][att])
        else:
            d = {}
            for att_id, att in self.att_list:
                if line_dict['attribute_id/id'].get(att):
                    d[att_id] = [line_dict['value_ids/id'][att]]
            self.data[line_dict['product_tmpl_id/id']] = d

    def generate_line(self):
        lines_header = ['id', 'product_tmpl_id/id', 'attribute_id/id', 'value_ids/id']
        lines_out = []
        for template_id, attributes in self.data.items():
            if not template_id:
                continue
            for attribute, values in attributes.items():
                line = [self.id_gen(template_id, attributes), template_id, attribute, ','.join(values)]
                lines_out.append(line)
        return lines_header, lines_out
