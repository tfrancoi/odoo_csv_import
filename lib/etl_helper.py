from internal.tools import to_m2o
from internal.exceptions import SkippingException
#TODO Import product variant


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