'''
Created on 9 sept. 2016

@author: Thibault Francois <francois.th@gmail.com>
'''

"""
    Data formatting tools
"""
def to_xmlid(name):
    return name.replace('.', '_').replace(',', '_').strip()

def list_to_xml_id(names):
    return '_'.join([to_xmlid(name) for name in names])

def to_m2o(PREFIX, value, default=''):
    if not value:
        return default
    return PREFIX + to_xmlid(value)

def to_m2m(PREFIX, value):
    if not value:
        return ''

    ids = []
    for val in value.split(','):
        if val.strip():
            ids.append(PREFIX + to_xmlid(val))
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
def add_parent(s, PREFIX, val, parent_val, default=None):
    default = default or []
    if val and parent_val and val.strip() != parent_val.strip():
        s.add(tuple([PREFIX + to_xmlid(val), PREFIX + to_xmlid(parent_val)] + default))

def add_m2o(s, PREFIX, value, default=None):
    default = default or []
    if value.strip():
        s.add(tuple([PREFIX + to_xmlid(value), value.strip()] + default))

def add_m2m(s, PREFIX, value, default=None):
    if not value.strip():
        return

    default = default or []
    for val in value.split(','):
        if val.strip():
            s.add(tuple([PREFIX + to_xmlid(val), val.strip()] + default))

class ReprWrapper(object):
    def __init__(self, repr_str, func):
        self._repr = repr_str
        self._func = func

    def __call__(self, *args, **kw):
        return self._func(*args, **kw)

    def __repr__(self):
        return self._repr
