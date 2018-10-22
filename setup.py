# -*- coding: utf-8 -*-
'''
Copyright (C) Thibault Francois

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as
published by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
Lesser General Lesser Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
'''

from setuptools import setup, find_packages

setup(name='odoo-import-export-client',
      version='2.1.0',
      install_requires=['odoo-client-lib', 'future', 'unicodecsv'],
      description='Library and script that allow to export and import data to Odoo using rpc api.',
      author='Thibault Francois',
      author_email='francois.th@gmail.com',
      url='https://github.com/tfrancoi/odoo_csv_import',
      packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
      scripts=['odoo_export_thread.py', 'odoo_import_thread.py'],
      long_description="See the home page for any information: https://github.com/tfrancoi/odoo_csv_import",
      keywords="odoo library import export thread python client lib web service",
      license="LGPLv3",
      classifiers=[
          "License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)",
          "Programming Language :: Python",
      ],
      )
