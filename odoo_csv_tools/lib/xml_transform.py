#-*- coding: utf-8 -*-
import transform
from collections import OrderedDict
from lxml import etree


class XMLProcessor(transform.Processor):
    def __init__(self, filename, root_node_path):
        self.root = etree.parse(filename)
        self.root_path = root_node_path
        self.file_to_write = OrderedDict()

    def process(self, mapping, filename_out, import_args, t='list', null_values=['NULL', False], verbose=True, m2m=False):
        """
            t, null_values, verbose, m2m are kept as arguments for compatibility but they are not use
            in XMLProcessor
        """
        header = mapping.keys()
        lines = []
        for r in self.root.xpath(self.root_path):
            line = [r.xpath(mapping[k])[0] for k in header]
            lines.append(line)
        self._add_data(header, lines, filename_out, import_args)
        return header, lines

    def split(self, split_fun):
        raise NotImplementedError("Method split not supported for XMLProcessor")


