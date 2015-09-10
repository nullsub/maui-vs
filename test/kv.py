# -*- coding: utf-8 -*-

import h5py as h5
import numpy as np
from maui.backend import context
import os
import threading

import hyperdex.client
import hyperdex.admin
import time 
import functools 
import argparse

parser = argparse.ArgumentParser("kv_store")
parser.add_argument("--kv_coordinator", action="store") 
args, _ = parser.parse_known_args()

#stackoverflow
def timeit(func):
    @functools.wraps(func)
    def newfunc(*args, **kwargs):
        startTime = time.time()
        func(*args, **kwargs)
        elapsedTime = time.time() - startTime
        print('function [{}] finished in {} ms'.format(
            func.__name__, int(elapsedTime * 1000)))
    return newfunc

space_count = 0
spacename = "kv"
kv_writer = False
last_put = None
if(not hasattr(context, 'comm' ) or context.rank == 0):
	kv_writer = True
	print("connecting to coordinator: " + args.kv_coordinator)
	#print("kv-flush-sec2")
	#print("kv-thread")
	print("kv-no-flush")
	client = hyperdex.client.Client(args.kv_coordinator, 1982)
	client.group_del(spacename + str(space_count), {})
	#admin = hyperdex.admin.Admin(args.kv_coordinator, 1982)
	#admin.add_space("space " + spacename + str(space_count) + " key name attributes value tolerate 0 failures")

def set_attrs(group_name, name, value):
	global last_put
	if(not kv_writer):
		return
	if(last_put is not None):
		last_put.wait()
	last_put = client.async_put(spacename + str(space_count), str(group_name) + "/" + name, {'value': str(value)})

#@timeit
def flush(filename, space):
	client = hyperdex.client.Client(args.kv_coordinator, 1982)
	h5_base = h5.File(filename+'.vsh5', 'r+', driver='sec2')
	for x in client.search(space, {}):
		the_set = h5_base.get(os.path.dirname(x['name']))
		the_set.attrs[os.path.basename(x['name'])] = np.string_(x['value'])
	h5_base.close()

def finalize(filename):
	global space_count
	global last_put
	if(hasattr(context, 'comm' ) and context.rank is not 0):
		return
	del_group = client.async_group_del(spacename + str(space_count+1), {})
	if(last_put is not None):
		last_put.wait()
		last_put = None
	#flush(filename, spacename + str(space_count))
	#f = threading.Thread(name='flush'+str(space_count), target=flush, args=(filename, spacename + str(space_count)))
	#f.start()
	space_count += 1
	del_group.wait()
