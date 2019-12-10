"""
Created on 14 sept. 2016

@author: mythrys
"""
import random
import sys

from const import EXEC

from odoo_csv_tools.lib import mapper
from odoo_csv_tools.lib import transform

if sys.version_info < (3, 0, 0):
    from builtins import range

if len(sys.argv) == 2:
    EXEC = sys.argv[1]

PARTNER_PREFIX = 'partner_generated'
TAG_PREFIX = 'partner_tag'
output = 'data/res.partner.generated.csv'
tag_output = 'data/res.partner.category.csv'
script = '0_partner_generated.sh'

tags = ["Tag %s" % i for i in range(0, 100)]

header = ['id', 'tags']
data = [[str(i), ','.join(tags[random.randint(0, 99)] for i in range(0, 5))] for i in range(0, 200)]

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
processor.process(tag_mapping, tag_output, {
    'worker': 1,
    'batch_size': 10,
    'model': 'res.partner.category',
}, m2m=True)
processor.process(mapping, output, {
    'worker': 4,
    'batch_size': 100,
    'model': 'res.partner',
})
processor.write_to_file(script, python_exe=EXEC, path='../', encoding="utf-8-sig")
