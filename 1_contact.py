# -*- coding: utf-8 -*-
import os
#Import toolkit
from lib import mapper
from lib.etl_helper import read_file, process_mapping, process_write_file
from lib.data_checker import check_id_validity, check_length_validity
#Import specific data needed for the import
from mapping import country_map, lang_map
from prefix import PARTNER_PREFIX


#STEP 1 : read the needed file(s)
head, data = read_file('origin%scontact.csv' % os.sep)
#DISPLAY header
import pprint
pprint.pprint(head)

#STEP 2 : Define the mapping for every object to import
mapping =  {
    'id' : mapper.m2o(PARTNER_PREFIX, 'Company_ID', skip=True),
    'name' : mapper.val('Company_Name', skip=True),
    'phone' : mapper.val('Phone'),
    'website' : mapper.val('www'),
    'street' : mapper.val('address1'),
    'city' : mapper.val('city'),
    'zip' : mapper.val('zip code'),
    'country_id/id' : mapper.map_val('country', country_map),
    'company_type' : mapper.const('company'),
    'customer' : mapper.bool_val('IsCustomer', ['1'], ['0']),
    'supplier' : mapper.bool_val('IsSupplier', ['1'], ['0']),
    'lang' : mapper.map_val('Language', lang_map)
}

#Step 3: Check data quality (Optional)
check_length_validity(12, data)
check_id_validity('Company_ID', "COM\d", head, data)

#Step 4: Process data
partner_header, partner_data = process_mapping(head, data, mapping)

#Step 5: Define output and import parameter
file_to_write = [
    {
        'filename' : 'data/res.partner.csv',
        'header': partner_header,
        'data' : partner_data,
        'worker' : 3, #OPTIONAL
        'batch_size' : 10, #OPTIONAL
    },
]

process_write_file("1_contact_import.sh", file_to_write)