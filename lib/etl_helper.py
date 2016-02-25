from csv_reader import UnicodeWriter, UnicodeReader
import csv
import re

"""
    Data formatting helper

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

"""

    File Manipulation
    
"""
def write_file(filename=None, header=None, data=None, fail=False, model="auto", launchfile="import_auto.sh", worker=1, batch_size=10, init=False):
    def get_model():
        if model == "auto":
            return filename.split('/')[-1][:-4]
        else:
            return model
    write_csv(filename, header, data)

    mode = init and 'w' or 'a'
    with open(launchfile, mode) as myfile:
        myfile.write("python odoo_import_thread.py -c conf/connection.conf --file=%s --model=%s --worker=%s --size=%s \n" % (filename, get_model(), worker, batch_size))
        if fail:
            myfile.write("python odoo_import_thread.py -c conf/connection.conf --fail --file=%s --model=%s --worker=%s --size=%s \n" % (filename, get_model(), worker, batch_size))
            
def write_csv(filename, header, data):
    file_result = open(filename, "wb")
    c = UnicodeWriter(file_result, delimiter=';', quoting=csv.QUOTE_ALL)
    c.writerow(header)
    for d in data:
        c.writerow(d)
    file_result.close()
    
def write_file_dict(filename, header, data):
    data_rows = []
    for k, val in data.iteritems():
        r = [val.get(h, '') for h in header]
        data_rows.append(r)
    write_csv(filename, header, data_rows)

def read_file(file_name, id_name):
    file_ref = open(file_name, 'r')
    reader = UnicodeReader(file_ref, delimiter=';')

    head = reader.next()
    res = {}
    for line in reader:
        if any(line):
            line_dict = dict(zip(head, line))
            res[line_dict[id_name]] = line_dict
    return res, head
    
def merge_file(master, child, field):
    res = {}
    for key, val in master.iteritems():
        data = dict(child.get(val[field], {}))
        new_dict = dict(val)
        new_dict.update(data)
        res[key] = new_dict
    return res
    
    
def merge_header(*args):
    old_header = [item for sublist in args for item in sublist]
    header = []
    for h in old_header:
        if h and h not in header:
            header.append(h)
    return header



def check_id_validity(id_field, pattern, header_in, data, null_values=['NULL']):
    regular = re.compile(pattern)
    i = 1
    for line in data:
        i+=1
        line = [s.strip() if s.strip() not in null_values else '' for s in line]
        line_dict = dict(zip(header_in, line))
        if not regular.match(line_dict[id_field]):
            print "Check Failed Id Validity", i, line_dict[id_field]
            continue
        
def check_length_validity(length, data):
    i = 1
    for line in data:
        i+=1
        if len(line) != length:
            print "Check Failed", i, "Line Length", len(line)
        
def check_number_line(line_number, data):
    if len(data) + 1 != line_number:
        print "Check Line Number Failed %s instead of %s" % (len(data) + 1, line_number)
        
def check_cell_len_max(cell_len, data):
    i = 1
    for line in data:
        i+=1
        for ele in line:
            if len(ele) > cell_len:
                print "Check Failed", i, "Cell Length", len(ele)
                print line

def process_mapping(header_in, data, mapping, t='list', null_values=['NULL'], verbose=True):
    """
        @param t: type of return, list or set 
    """
    lines_out = [] if t == 'list' else set()
    i = 1
    for line in data:
        i+=1
        line = [s.strip() if s.strip() not in null_values else '' for s in line]
        line_dict = dict(zip(header_in, line))
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

def line_id(template_id, values):
    prefix, name = template_id.split('.')
    return to_m2o(prefix + '_LINE.', template_id)

def process_attribute_mapping(header_in, data, mapping, line_mapping, attributes_list, ATTRIBUTE_PREFIX, id_gen_fun=None, null_values=['NULL']):
    """
        Mapping : name is mandatory vat_att(attribute_list)
    """
    def generate_attribute_lines():
        header = ['id', 'name']
        data = [[to_m2o(ATTRIBUTE_PREFIX, att), att] for att in attributes_list]
        return header, data

    def add_value_line(values_out, line):
        for att in attributes_list:
            value_name = line[mapping.keys().index('name')].get(att)
            if value_name:
                line_value = [ele[att] if isinstance(ele, dict) else ele for ele in line]
                values_out.add(tuple(line_value))

    id_gen_fun = id_gen_fun or line_id

    values_header = mapping.keys()
    values_data = set()

    attribute_header, attribute_data = generate_attribute_lines()
    att_data = attribute_line_dict(attribute_data, id_gen_fun)
    for line in data:
        line = [s.strip() if s.strip() not in null_values else '' for s in line]
        line_dict = dict(zip(header_in, line))
        line_out = [mapping[k](line_dict) for k in mapping.keys()]

        add_value_line(values_data, line_out)
        values_lines = [line_mapping[k](line_dict) for k in line_mapping.keys()]
        att_data.add_line(values_lines, line_mapping.keys())

    line_header, line_data = att_data.generate_line()
    return attribute_header, attribute_data, values_header, values_data, line_header, line_data

def process_write_file(launch_file, info_list, fail=True):
    init = True
    for info in info_list:
        info_copy = dict(info)
        info_copy.update({
            'model' : info.get('model', 'auto'),
            'init' : init,
            'launchfile' : launch_file,
            'fail' : fail,
        })

        write_file(**info_copy)
        init = False

class SkippingException(Exception):
    pass

class attribute_line_dict:
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
        for template_id, attributes in self.data.iteritems():
            if not template_id:
                continue
            for attribute, values in attributes.iteritems(): 
                line = [self.id_gen(template_id, attributes), template_id, attribute, ','.join(values)]
                lines_out.append(line)
        return lines_header, lines_out