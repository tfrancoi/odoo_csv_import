#-*- coding: utf-8 -*-
'''
Created on 19 august 2016

@author: Thibault Francois <francois.th@gmail.com>
'''

import threading

class RpcThread(object):

    def __init__(self, max_connection):
        self.semaphore = threading.BoundedSemaphore(max_connection)
        self.max_thread_semaphore = threading.BoundedSemaphore(max_connection * 4)
        self.thread_list = []

    def spawn_thread(self, fun, args, kwarg=None):
        def wrapper(args, kwarg):
            kwarg = kwarg or {}
            self.semaphore.acquire()
            try:
                fun(*args, **kwarg)
            except:
                self.semaphore.release()
                self.max_thread_semaphore.release()
                raise
            self.semaphore.release()
            self.max_thread_semaphore.release()
        self.max_thread_semaphore.acquire()

        thread = threading.Thread(None, wrapper, None, [args, kwarg], {})
        thread.start()
        self.thread_list.append(thread)

    def wait(self):
        for t in self.thread_list:
            t.join()

    def thread_number(self):
        return len(self.thread_list)
