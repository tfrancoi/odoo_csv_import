'''
Created on 7 avr. 2016

@author: odoo
'''
#from __future__ import absolute_import
import sys
if sys.version_info >= (3, 0, 0):
    from xmlrpc.client import Fault
else:
    from xmlrpclib import Fault

from time import time
from . internal.rpc_thread import RpcThread

class InvoiceWorkflowV9():
    def __init__(self, connection, field, status_map, paid_date_field, payment_journal, max_connection=4):
        """
            @param connection : need to use a jsonrpc connection
            @param field: the that contains the state imported from legacy data
            @param status_map: dict that contains the mapping between the odoo invoice status and legacy system status
                                the value should be a list
                            {
                                'open' : ['satus1'],
                                'paid' : ['status2', 'status3'],
                                'cancel' : ...
                                'proforma' :
                            }
        """
        self.connection = connection
        self.invoice_obj = connection.get_model('account.invoice')
        self.payement_obj = connection.get_model('account.payment')
        self.account_invoice_tax = self.connection.get_model('account.invoice.tax')
        self.field = field
        self.status_map = status_map
        self.paid_date = paid_date_field
        self.payment_journal = payment_journal
        self.max_connection = max_connection

    def display_percent(self, i, percent_step, total):
        if i % percent_step == 0:
            print("%s%% : %s/%s time %s sec" % (round(i / float(total) * 100, 2), i, total, time() - self.time))

    def set_tax(self):
        def create_tax(invoice_id):
            taxes = self.invoice_obj.get_taxes_values(invoice_id)
            for tax in taxes.values():
                self.account_invoice_tax.create(tax)

        invoices = self.invoice_obj.search([('state', '=', 'draft'),
                                            ('type', '=', 'out_invoice'),
                                            ('tax_line_ids', '=', False)])
        total = len(invoices)
        percent_step = int(total / 5000) or 1
        self.time = time()
        rpc_thread = RpcThread(self.max_connection)
        print("Compute Tax %s invoice" %  total)
        for i, invoice_id in enumerate(invoices):
            self.display_percent(i, percent_step, total)
            rpc_thread.spawn_thread(create_tax, [invoice_id])
        rpc_thread.wait()

    def validate_invoice(self):
        invoice_to_validate = self.invoice_obj.search([(self.field, 'in', self.status_map['open'] + self.status_map['paid']),
                                                       ('state', '=', 'draft'),
                                                       ('type', '=', 'out_invoice')])
        total = len(invoice_to_validate)
        percent_step = int(total / 5000) or 1
        rpc_thread = RpcThread(1)
        print("Validate %s invoice" %  total)
        self.time = time()
        for i, invoice_id in enumerate(invoice_to_validate):
            self.display_percent(i, percent_step, total)
            fun = self.connection.get_service('object').exec_workflow
            rpc_thread.spawn_thread(fun, [self.connection.database,
                                          self.connection.user_id,
                                          self.connection.password,
                                          'account.invoice',
                                          'invoice_open',
                                          invoice_id])
        rpc_thread.wait()

    def proforma_invoice(self):
        invoice_to_proforma = self.invoice_obj.search([(self.field, 'in', self.status_map['proforma']),
                                                       ('state', '=', 'draft'),
                                                       ('type', '=', 'out_invoice')])
        total = len(invoice_to_proforma)
        percent_step = int(total / 100) or 1
        self.time = time()
        rpc_thread = RpcThread(self.max_connection)
        print("Pro Format %s invoice" %  total)
        for i, invoice_id in enumerate(invoice_to_proforma):
            self.display_percent(i, percent_step, total)
            fun = self.connection.get_service('object').exec_workflow()
            rpc_thread.spawn_thread(fun, [self.connection.database,
                                                         self.connection.user_id,
                                                         self.connection.password,
                                                         'account.invoice',
                                                         'invoice_proforma2',
                                                         invoice_id], {})
        rpc_thread.wait()

    def paid_invoice(self):
        def pay_single_invoice(data_update, wizard_context):
            data = self.payement_obj.default_get(["communication", "currency_id", "invoice_ids",
                                                 "payment_difference", "partner_id", "payment_method_id",
                                                 "payment_difference_handling", "journal_id",
                                                 "state", "writeoff_account_id", "payment_date",
                                                 "partner_type", "hide_payment_method",
                                                 "payment_method_code", "partner_bank_account_id",
                                                 "amount", "payment_type"], context=wizard_context)
            data.update(data_update)
            wizard_id = self.payement_obj.create(data, context=wizard_context)
            try:
                self.payement_obj.post([wizard_id], context=wizard_context)
            except Fault:
                pass


        invoice_to_paid = self.invoice_obj.search_read([(self.field, 'in', self.status_map['paid']), ('state', '=', 'open'), ('type', '=', 'out_invoice')],
                                                       [self.paid_date, 'date_invoice'])
        total = len(invoice_to_paid)
        percent_step = int(total / 1000) or 1
        self.time = time()
        rpc_thread = RpcThread(self.max_connection)
        print("Paid %s invoice" %  total)
        for i, invoice in enumerate(invoice_to_paid):
            self.display_percent(i, percent_step, total)
            wizard_context = {
                              'active_id' : invoice['id'],
                              'active_ids' : [invoice['id']],
                              'active.model' : 'account.invoice',
                              'default_invoice_ids' : [(4, invoice['id'], 0)],
                              'type' : "out_invoice",
                              "journal_type":"sale"
                            }
            data_update = {
                           'journal_id' : self.payment_journal, #payement journal
                           'payment_date' : invoice[self.paid_date] or invoice['date_invoice'],
                           'payment_method_id' :  1,
                        }
            rpc_thread.spawn_thread(pay_single_invoice, [data_update, wizard_context], {})
        rpc_thread.wait()

    def rename(self, name_field):
        invoice_to_paid = self.invoice_obj.search_read([(name_field, '!=', False),(name_field, '!=', '0.0'),('state', '!=', 'draft'), ('type', '=', 'out_invoice')],
                                                       [name_field])
        total = len(invoice_to_paid)
        percent_step = int(total / 1000) or 1
        self.time = time()
        rpc_thread = RpcThread(int(self.max_connection * 1.5))
        print("Rename %s invoice" %  total)
        for i, invoice in enumerate(invoice_to_paid):
            self.display_percent(i, percent_step, total)
            rpc_thread.spawn_thread(self.invoice_obj.write, [invoice['id'], {'number' : invoice[name_field], name_field : False}], {})
        rpc_thread.wait()
