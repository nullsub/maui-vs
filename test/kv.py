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
self.admin = hyperdex.admin.Admin(args.kv_coordinator, 1982)
self.client = hyperdex.client.Client(args.kv_coordinator, 1982)
print(self.admin.list_spaces())

#class kv_store:
#	def __init__(self):
def set_attrs(group_name, name, value):
	key = str(group_name) + "/" + str(name)
	print("storing: key: " + key + ", val: " + str(value))
	self.client.put(self.spacename, key, {'value': str(value)})

def print_kv():
	print("getting kv_data:")
	for x in self.client.search(self.spacename, {}):
		print x			

def finish():
	print("saving in hdf5")
		
