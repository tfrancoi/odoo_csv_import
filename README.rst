Odoo CSV Import Export Library
==============================
This library provides tools to easily and quickly import data into Odoo or export data from Odoo using CSV file. 
It also provide a framework to manipulate date from csv.

Requirements
--------------
* openerp-client-lib

Usage
-----

Importing
^^^^^^^^^

Import data in batch and in parallel.


**usage:**

.. code-block:: bash

   odoo_import_thread.py [-h] -c CONFIG --file FILENAME --model MODEL
                             [--worker WORKER] [--size BATCH_SIZE]
                             [--skip SKIP] [--fail] [-s SEPARATOR]
                             [--groupby SPLIT] [--ignore IGNORE] [--check]
                             [--context CONTEXT] [--o2m]``

*Example*: importing partners from a file

.. code-block:: bash

   ./odoo_import_thread.py --config yourconfig.cfg --model=res.partner --file sourcefile.csv

**optional arguments:**

-h, --help            show this help message and exit
-c CONFIG, --config CONFIG
                    Configuration File that contains connection parameters
--file FILENAME       File to import
--model MODEL         Model to import
--worker WORKER       Number of simultaneous connection
--size BATCH_SIZE     Number of line to import per connection
--skip SKIP           Skip until line [SKIP]
--fail                Fail mode
-s SEPARATOR, --sep SEPARATOR
                    CSV separator
--groupby SPLIT       Group data per batch with the same value for the given
                    column in order to avoid concurrent update error
--ignore IGNORE       list of column separate by comma. Those column will be
                    remove from the import request
--check               Check if record are imported after each batch.
--context CONTEXT     context that will be passed to the load function, need
                    to be a valid python dict
--o2m                 When you want to import o2m field, don't cut the batch
                    until we find a new id

Exporting
^^^^^^^^^

Export data in batch and in parallel

**usage:** 

.. code-block:: bash

   odoo_export_thread.py [-h] -c CONFIG --file FILENAME --model MODEL
                             --field FIELDS [--domain DOMAIN]
                             [--worker WORKER] [--size BATCH_SIZE]
                             [-s SEPARATOR] [--context CONTEXT]

*Example*: exporting partner name and street to a file

.. code-block:: bash

   ./odoo_export_thread.py -c yourconfig.cfg --model=res.partner --field=name,street --file outputfile.csv

**optional arguments:**

-h, --help            show this help message and exit
-c CONFIG, --config CONFIG
                    Configuration File that contains connection parameters
--file FILENAME       Output File
--model MODEL         Model to Export
--field FIELDS        Fields to Export
--domain DOMAIN       Filter
--worker WORKER       Number of simultaneous connection
--size BATCH_SIZE     Number of line to import per connection
-s SEPARATOR, --sep SEPARATOR
                    CSV separator
--context CONTEXT     context that will be passed to the load function, need
                    to be a valid python dict

