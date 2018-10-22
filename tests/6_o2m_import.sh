#!/usr/bin/env bash
$1 ../odoo_import_thread.py --file=origin/res.partner_o2m.csv --model='res.partner' --size=1 --worker=1 --conf=conf/connection.conf --o2m
