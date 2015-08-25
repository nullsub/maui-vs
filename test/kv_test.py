# -*- coding: utf-8 -*-

from __future__ import division

import sys

import kv as k
import numpy as np
#import h5py as h5

kv = k.kv_store()
str = np.string_("time")
print("string is " + str)
kv.print_kv()
kv.set_attrs("/time/", "vsType", str.encode("utf8"))
kv.set_attrs("/time/", "vsType1", str.encode("utf8"))
kv.set_attrs("/time1/", "vsType", str.encode("utf8"))
kv.print_kv()
