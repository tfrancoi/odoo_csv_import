#!/usr/bin/env bash
$1 ../odoo_export_thread.py -c conf/connection.conf --file=data/res.partner.exported.csv --model=res.partner --worker=4 --size=200 --domain="[]" --field="id,name,phone,website,street,city,country_id/id" --sep=";"
