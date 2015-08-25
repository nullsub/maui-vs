# -*- coding: utf-8 -*-

from __future__ import division

import hyperdex.client
import hyperdex.admin

import argparse
parser = argparse.ArgumentParser("kv_store")
parser.add_argument("--kv_coordinator", action="store") 
args, _ = parser.parse_known_args()

spacename="kv"
print("connecting to coordinator: " + args.kv_coordinator)
admin = hyperdex.admin.Admin(args.kv_coordinator, 1982)
client = hyperdex.client.Client(args.kv_coordinator, 1982)
print(admin.list_spaces())

#class kv_store:
#	def __init__(self):
def set_attrs(group_name, name, value):
	key = str(group_name) + "/" + str(name)
	print("storing: key: " + key + ", val: " + str(value))
	client.put(spacename, key, {'value': str(value)})

def flush(hdf5_file):
	print("flushing metadata to file")
	for x in client.search(spacename, {}):
		print x			

def print_kv():
	print("getting kv_data:")
	for x in client.search(spacename, {}):
		print x			

