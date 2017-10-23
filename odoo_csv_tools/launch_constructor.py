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
import os


def launchfile_write(file_to_write, script_filename, fail=False, append=False, python_exe=None, path=None, conf_file=None):
    init = not append
    for _, info in file_to_write.items():
        model = info.get('model', 'auto')
        filename = info.get('filename')
        args = {
            'filename': filename,
            'fail': fail,
            'model': model if model != 'auto' else filename.split(os.sep)[-1][:-4],
            'launchfile': script_filename or 'import_auto.sh',
            'init': init,
        }
        if 'groupby' in info.keys():
            args['groupby'] = info.get('groupby')
        if 'sep' in info.keys():
            args['sep'] = info.get('sep')
        if 'context' in info.keys():
            args['context'] = info.get('context')
        if python_exe:
            args['python_exe'] = python_exe
        if conf_file:
            args['conf_file'] = conf_file
        if path:
            args['path'] = path
        write_line(**args)
        init = False


def write_line(filename=None,
               fail=False,
               model=None,
               launchfile=None,
               worker=1,
               batch_size=10,
               init=False,
               conf_file=None,
               groupby='',
               sep=";",
               python_exe='python',
               path='./',
               context=None):

    conf_file = conf_file or "%s%s%s" % ('conf', os.sep, 'connection.conf')
    context = '--context="%s"' % str(context) if context else ''
    line = "{python_exe} {path}odoo_import_thread.py -c {conf_file} --file={filename} --model={model} --worker={worker} --size={batch_size} --groupby={groupby} --sep=\"{sep}\" {context}\n".format(**locals())
    line_fail = "{python_exe} {path}odoo_import_thread.py -c {conf_file} --fail --file={filename} --model={model} --sep=\"{sep}\" {context}\n".format(**locals())
    mode = init and 'w' or 'a'
    with open(launchfile, mode) as myfile:
        myfile.write(line_fail) if fail else myfile.write(line)
