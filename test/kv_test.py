# -*- coding: utf-8 -*-

import kv
import h5py as h5

base = h5.File("file"+'.vsh5', 'w', driver='sec2')
a_group = h5.Group.create_group(base, "time")
kv.set_attrs(a_group.name, "aa", str.encode("utf8"))
kv.set_attrs(a_group.name, "vsType1", str.encode("utf8"))
a_group = h5.Group.create_group(base, "time1")
kv.set_attrs(a_group.name, "vsType", str.encode("utf8"))
kv.set_attrs(a_group.name, "vsType1", str.encode("utf8"))
a_group = h5.Group.create_group(base, "abc")
kv.set_attrs(a_group.name, "vsType1", str.encode("utf8"))
a_group = h5.Group.create_group(base, "qabc")
kv.set_attrs(a_group.name, "vsType1", str.encode("utf8"))
base.close
kv.finalize("file")
