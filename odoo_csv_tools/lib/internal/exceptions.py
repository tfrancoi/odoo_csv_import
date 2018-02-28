'''
Created on 9 sept. 2016

@author: Thibault Francois <francois.th@gmail.com>
'''

class SkippingException(Exception):
    def __init__(self, message):
        self.message = message
