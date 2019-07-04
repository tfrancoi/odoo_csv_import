#!/usr/bin/env bash
$1 ../odoo_convert_path_to_image.py --path=./origin/img/ -f Image origin/contact.csv
$1 ../odoo_convert_url_to_image.py -f Image origin/contact_url.csv

