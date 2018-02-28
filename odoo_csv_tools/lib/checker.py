# -*- coding: utf-8 -*-
'''
Created on 29 feb. 2016

@author: Thibault Francois
'''
#TODO
import re

def id_validity_checker(id_field, pattern, null_values=['NULL']):
    def check_id_validity(header, data):
        regular = re.compile(pattern)
        res = True
        for i, line in enumerate(data):
            line = [s.strip() if s.strip() not in null_values else '' for s in line]
            line_dict = dict(zip(header, line))
            if not regular.match(line_dict[id_field]):
                print("Check Failed Id Validity", i+1, line_dict[id_field])
                res = False
        return res
    return check_id_validity

def line_length_checker(length):
    def check_line_length(header, data):
        i = 1
        res = True
        for line in data:
            i+=1
            if len(line) != length:
                print("Check Failed", i, "Line Length", len(line))
                res = False
        return res
    return check_line_length

def line_number_checker(line_number):
    def check_line_numner(header, data):
        if len(data) + 1 != line_number:
            print("Check Line Number Failed %s instead of %s" % (len(data) + 1, line_number))
            return False
        else:
            return True
    return check_line_numner

def cell_len_checker(max_cell_len):
    def check_max_cell_len(header, data):
        res = True
        for i, line in enumerate(data):
            for ele in line:
                if len(ele) > max_cell_len:
                    print("Check Failed", i + 1, "Cell Length", len(ele))
                    print(line)
                    res = False
        return res
    return check_max_cell_len
