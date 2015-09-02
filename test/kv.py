# -*- coding: utf-8 -*-

import h5py as h5
import numpy as np
from maui.backend import context
import os

import hyperdex.client
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

spacename = "kv" 
kv_writer = False
last_put = None
if(context.rank == 0):
	kv_writer = True
	print("connecting to coordinator: " + args.kv_coordinator)
	client = hyperdex.client.Client(args.kv_coordinator, 1982)

def set_attrs(group_name, name, value):
	global last_put
	if(not kv_writer):
		return
	if(last_put is not None):
		last_put.wait()
	last_put = client.async_put(spacename, str(group_name) + "/" + name, {'value': str(value)})

@timeit
def flush(hdf5_base):
	if(last_put is not None):
		last_put.wait()
	for x in client.search(spacename, {}):
		the_set = hdf5_base.get(os.path.dirname(x['name']))
		the_set.attrs[os.path.basename(x['name'])] = np.string_(x['value'])

@timeit
def finalize(hdf5_base):
	flush(hdf5_base)
	del_group = client.async_group_del(spacename, {})
	hdf5_base.close()
	del_group.wait()
