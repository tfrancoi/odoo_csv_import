'''
Created on 14 sept. 2016

@author: mythrys
'''
import random

from odoo_csv_tools.lib import mapper
from odoo_csv_tools.lib import transform

PARTNER_PREFIX = 'partner_generated'
TAG_PREFIX = 'partner_tag'
output = 'data/res.partner.generated.csv'
tag_output = 'data/res.partner.category.csv'
script = '1_partner_split.sh'

tags = ["Tag %s" % i for i in range(0, 100)]

header = ['id', 'tags']
data = [[str(i), ','.join(tags[random.randint(0, 99)] for i in range(0, 5))] for i in range(0, 10000)]

mapping = {
    'id': mapper.m2o(PARTNER_PREFIX, 'id'),
    'name': mapper.val('id', postprocess=lambda x: "Partner %s" % x),
    'phone': mapper.val('id', postprocess=lambda x: "0032%s" % (int(x) * 11)),
    'website': mapper.val('id', postprocess=lambda x: "http://website-%s.com" % x),
    'street': mapper.val('id', postprocess=lambda x: "Street %s" % x),
    'city': mapper.val('id', postprocess=lambda x: "City %s" % x),
    'zip': mapper.val('id', postprocess=lambda x: ("%s" % x).zfill(6)),
    'country_id/id': mapper.const('base.be'),
    'company_type': mapper.const('company'),
    'customer': mapper.val('id', postprocess=lambda x: str(int(x) % 2)),
    'supplier': mapper.val('id', postprocess=lambda x: str((int(x) + 1) % 2)),
    'lang': mapper.const('English'),
    'category_id/id': mapper.m2m(TAG_PREFIX, 'tags')
}

tag_mapping = {
    'id': mapper.m2m_id_list(TAG_PREFIX, 'tags'),
    'name': mapper.m2m_value_list('tags'),
    'parent_id/id': mapper.const('base.res_partner_category_0'),
}

processor = transform.Processor(header=header, data=data)
p_dict = processor.split(mapper.split_line_number(1000))  # Useless just for coverage
p_dict = processor.split(mapper.split_file_number(8))
processor.process(tag_mapping, tag_output, {
    'worker': 1,  # OPTIONAL
    'batch_size': 10,  # OPTIONAL
    'model': 'res.partner.category',
}, m2m=True)
processor.write_to_file(script, path='../')
for index, p in p_dict.items():
    p.process(mapping, '%s.%s' % (output, index), {
        'worker': 4,  # OPTIONAL
        'batch_size': 100,  # OPTIONAL
        'model': 'res.partner',
    })
    p.write_to_file(script, path='../', append=True)
