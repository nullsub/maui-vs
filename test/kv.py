# -*- coding: utf-8 -*-

from __future__ import division

import hyperdex.client
import hyperdex.admin

import h5py as h5
import numpy as np
import time 
import os

import argparse
parser = argparse.ArgumentParser("kv_store")
parser.add_argument("--kv_coordinator", action="store") 
args, _ = parser.parse_known_args()

spacename = "kv_" + str(int(time.time()))
print("connecting to coordinator: " + args.kv_coordinator)
print("using spacename " + spacename)
admin = hyperdex.admin.Admin(args.kv_coordinator, 1982)
admin.add_space("space " + spacename + " key name attributes value")
client = hyperdex.client.Client(args.kv_coordinator, 1982)
print(admin.list_spaces())

def set_attrs(group_name, name, value):
	key = str(group_name) + "/" + str(name)
	#print("storing: key: " + key + ", val: " + str(value))
	client.put(spacename, key, {'value': str(value)})

def flush(hdf5_base):
	print("flushing kv data")
	for x in client.search(spacename, {}):
		print "setting " + os.path.dirname(x['name']) + "/" + os.path.basename(x['name']) + " to " + x['value']
		the_set = hdf5_base.get(os.path.dirname(x['name']))
		the_set.attrs[os.path.basename(x['name'])] = np.string_(x['value'])

def print_kv():
	print("getting kv_data:")
	for x in client.search(spacename, {}):
		print x			
