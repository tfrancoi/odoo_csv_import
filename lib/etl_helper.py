from csv_reader import UnicodeWriter
import csv

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

def write_file(filename, header, data, fail=False, model="auto", launchfile="import_auto.sh", worker=1, batch_size=10, init=False):
    def get_model():
        if model == "auto":
            return filename.split('/')[-1][:-4]
        else:
            return model

    file_result = open(filename, "wb")
    c = UnicodeWriter(file_result, delimiter=';', quoting=csv.QUOTE_ALL)
    c.writerow(header)
    for d in data:
        c.writerow(d)
    file_result.close()

    mode = init and 'w' or 'a'
    with open(launchfile, mode) as myfile:
        myfile.write("python odoo_import_thread.py -c conf/connection.conf --file=%s --model=%s --worker=%s --size=%s \n" % (filename, get_model(), worker, batch_size))
        if fail:
            myfile.write("python odoo_import_thread.py -c conf/connection.conf --fail --file=%s --model=%s --worker=%s --size=%s \n" % (filename, get_model(), worker, batch_size))