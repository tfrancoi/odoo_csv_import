# -*- coding: utf-8 -*-
import sys

import os
from const import EXEC

from odoo_csv_tools.lib import mapper
from odoo_csv_tools.lib.transform import ProductProcessorV10

if len(sys.argv) == 2:
    EXEC = sys.argv[1]

TEMPLATE_PREFIX = "PRODUCT_TEMPLATE"
PRODUCT_PREFIX = "PRODUCT_PRODUCT"
CATEGORY_PREFIX = "PRODUCT_CATEGORY"

ATTRIBUTE_PREFIX = "PRODUCT_ATTRIBUTE"
ATTRIBUTE_VALUE_PREFIX = "PRODUCT_ATTRIBUTE_VALUE"
ATTRIBUTE_LINE_PREFIX = "PRODUCT_ATTRIBUTE_LINE"

context = {'create_product_variant': True, 'tracking_disable': True}

# STEP 1 : read the needed file(s)
processor = ProductProcessorV10('origin%sproduct.csv' % os.sep, delimiter=',')

# STEP 2 : Category and Parent Category
categ_parent_map = {
    'id': mapper.m2o(CATEGORY_PREFIX, 'categoy'),
    'name': mapper.val('categoy'),
}

categ_map = {
    'id': mapper.m2o(CATEGORY_PREFIX, 'Sub Category'),
    'parent_id/id': mapper.m2o(CATEGORY_PREFIX, 'categoy'),
    'name': mapper.val('Sub Category'),
}

processor.process(categ_parent_map, 'data%sproduct.category.parent.csv' % os.sep, {'worker': 1, 'batch_size': 5,
                                                                                   'model': 'product.category'}, 'set')
processor.process(categ_map, 'data%sproduct.category.csv' % os.sep, {'worker': 1, 'batch_size': 20}, 'set')

# STEP 3 : Product Template mapping
template_map = {
    'id': mapper.m2o(TEMPLATE_PREFIX, 'ref'),
    'categ_id/id': mapper.m2o(CATEGORY_PREFIX, 'Sub Category'),
    'standard_price': mapper.num('cost'),
    'lst_price': mapper.num('public_price'),
    'default_code': mapper.val('ref'),
    'name': mapper.val('name'),
}
processor.process(template_map, 'data%sproduct.template.csv' % os.sep, {'worker': 4, 'batch_size': 10,
                                                                        'context': context}, 'set')

# STEP 4: Attribute List
attribute_list = ['Color', 'Gender', 'Size_H', 'Size_W']
processor.process_attribute_data(attribute_list, ATTRIBUTE_PREFIX, 'data%sproduct.attribute.csv' % os.sep,
                                 {'worker': 4, 'batch_size': 10,
                                  'context': context})
# STEP 5: Attribute Value
attribue_value_mapping = {
    'id': mapper.m2m_id_list(ATTRIBUTE_VALUE_PREFIX, *[mapper.concat_field_value_m2m('_', f) for f in attribute_list]),
    'name': mapper.m2m_value_list(*attribute_list),
    'attribute_id/id': mapper.m2m_id_list(ATTRIBUTE_PREFIX, *[mapper.field(f) for f in attribute_list]),
}
processor.process(attribue_value_mapping, 'data%sproduct.attribute.value.csv' % os.sep, {'worker': 3, 'batch_size': 50,
                                                                                         'context': context,
                                                                                         'groupby': 'attribute_id/id'},
                  m2m=True)

# STEP 6: Attribute Value Line
line_mapping = {
    'id': mapper.m2m_id_list(ATTRIBUTE_LINE_PREFIX,
                             *[mapper.concat_mapper_all('_', mapper.field(f), mapper.val('ref')) for f in
                               attribute_list]),
    'product_tmpl_id/id': mapper.m2o(TEMPLATE_PREFIX, 'ref'),
    'attribute_id/id': mapper.m2m_id_list(ATTRIBUTE_PREFIX, *[mapper.field(f) for f in attribute_list]),
    'value_ids/id': mapper.m2m_id_list(ATTRIBUTE_VALUE_PREFIX,
                                       *[mapper.concat_field_value_m2m('_', f) for f in attribute_list]),
}
context['update_many2many'] = True
processor.process(line_mapping, 'data%sproduct.attribute.line.csv' % os.sep, {'worker': 3, 'batch_size': 50,
                                                                              'context': dict(context),
                                                                              'groupby': 'product_tmpl_id/id'},
                  m2m=True)
context.pop('update_many2many')

# STEP 7: Product Variant
product_mapping = {
    'id': mapper.m2o_map(PRODUCT_PREFIX, mapper.concat('_', 'barcode', 'Color', 'Gender', 'Size_H', 'Size_W'),
                         skip=True),
    'barcode': mapper.val('barcode'),
    'product_tmpl_id/id': mapper.m2o(TEMPLATE_PREFIX, 'ref'),
    'attribute_value_ids/id': mapper.m2m_attribute_value(ATTRIBUTE_VALUE_PREFIX, 'Color', 'Gender', 'Size_H', 'Size_W'),
    'default_code': mapper.val('ref'),
    'standard_price': mapper.num('cost'),
}
processor.process(product_mapping, 'data%sproduct.product.csv' % os.sep, {'worker': 3, 'batch_size': 50,
                                                                          'groupby': 'product_tmpl_id/id',
                                                                          'context': context}, 'set')

# #Step 8: Define output and import parameter
processor.write_to_file("4_product_import.sh", python_exe=EXEC, path='../')
