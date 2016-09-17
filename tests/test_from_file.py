# -*- coding: utf-8 -*-
import os
#Import toolkit
from lib import mapper
from lib.transform import Processor
#from lib.etl_helper import read_file, process_mapping, process_write_file
#from lib.data_checker import check_id_validity, check_length_validity
#Import specific data needed for the import

lang_map = {
    '' : '',
    'French' : u'French (BE) / Français (BE)',
    'English' : u'English',
    'Dutch' : u'Dutch / Nederlands',
}

country_map = {
    'Belgique' : 'base.be',
    'BE' : 'base.be',
    'FR' : 'base.fr',
    'U.S' : 'base.us',
    'US' : 'base.us',
    'NL' : 'base.nl',
}

PARTNER_PREFIX = "TEST_PARTNER"

#STEP 1 : read the needed file(s)
processor = Processor('origin%scontact.csv' % os.sep)
#Print o2o mapping 
import pprint
pprint.pprint(processor.get_o2o_mapping())

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
#check_length_validity(12, data)
#check_id_validity('Company_ID', "COM\d", head, data)

#Step 4: Process data
processor.process(mapping, 'data%sres.partner.csv' % os.sep, { 'worker' : 2, 'batch_size' : 5}, 'set')

#Step 5: Define output and import parameter
processor.write_to_file("2_contact_import.sh", python_exe='python-coverage run -a', path='../')
