#!/usr/bin/env bash
#Need to launch odoo database accessible with the configuration given in conf/connection.conf
#Modules contacts need to be installed

rm -rf data
rm -rf htmlcov
rm 0_partner_generated.sh
rm 1_partner_split.sh
rm 2_contact_import.sh
rm 3_product_import.sh
rm 4_product_import.sh
rm .coverage
rm error.log
rm out.csv
