# -*- coding: utf-8 -*-

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

spacename = "kv" 
#spacename = "kv_" + str(int(time.time()))
print("1connecting to coordinator: " + args.kv_coordinator)
#admin = hyperdex.admin.Admin(args.kv_coordinator, 1982)
#admin.add_space("space " + spacename + " key name attributes value")
#print(admin.list_spaces())
client = hyperdex.client.Client(args.kv_coordinator, 1982)

def set_attrs(group_name, name, value):
	key = str(group_name) + "/" + str(name)
#	print("storing: key: " + key + ", val: " + str(value))
	client.put(spacename, key, {'value': str(value)})

def flush(hdf5_base):
	for x in client.search(spacename, {}):
	#	print "setting " + os.path.dirname(x['name']) + "/" + os.path.basename(x['name']) + " to " + x['value']
		the_set = hdf5_base.get(os.path.dirname(x['name']))
		the_set.attrs[os.path.basename(x['name'])] = np.string_(x['value'])
	print("finished flush")

def clear():
	client.group_del(spacename, {})

def print_kv():
	print("printing kv_data:")
	for x in client.search(spacename, {}):
		print x			
