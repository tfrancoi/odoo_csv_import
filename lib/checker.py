# -*- coding: utf-8 -*-
'''
Created on 29 feb. 2016

@author: Thibault Francois
'''
#TODO
import re

def check_id_validity(id_field, pattern, header_in, data, null_values=['NULL']):
    regular = re.compile(pattern)
    i = 1
    for line in data:
        i+=1
        line = [s.strip() if s.strip() not in null_values else '' for s in line]
        line_dict = dict(zip(header_in, line))
        if not regular.match(line_dict[id_field]):
            print "Check Failed Id Validity", i, line_dict[id_field]
            continue

def check_length_validity(length, data):
    i = 1
    for line in data:
        i+=1
        if len(line) != length:
            print "Check Failed", i, "Line Length", len(line)

def check_number_line(line_number, data):
    def check_line_numner(header, data):
        if len(data) + 1 != line_number:
            print "Check Line Number Failed %s instead of %s" % (len(data) + 1, line_number)
            return False
        else:
            return True
    return check_line_numner

def check_cell_len_max(cell_len):
    def check_cell_len_fun(header, data):
        res = True
        i = 1
        for line in data:
            i+=1
            for ele in line:
                if len(ele) > cell_len:
                    print "Check Failed", i, "Cell Length", len(ele)
                    print line
                    res = False
        return res
    return check_cell_len_fun
