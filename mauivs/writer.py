# -*- coding: utf-8 -*-

from __future__ import division

__author__ = 'christoph.statz <at> tu-dresden.de'

import numpy as np
from maui.backend import context
from .helper import *
import kv

import argparse
parser = argparse.ArgumentParser("maui-vs")
parser.add_argument("--h5collective", action='store_true')
args, _ = parser.parse_known_args()

H5P_COLLECTIVE = False

if args.h5collective:
    H5P_COLLECTIVE = True


class VSWriter(object):

    def __init__(self, domains, data, meta_data, var_name, var_type, var_unit=None, local_only=False):

        self.__domains = domains
        self.__data = data
        self.__meta_data = meta_data
        self.__name = var_name
        self.__type = var_type
        self.__var_unit = var_unit
        self.__local_only = local_only

    def write(self, base, base_name=""):

        mesh_groups = dict()
        datasets = dict()

        if self.__local_only:
            keys = self.__data.keys()
        else:
            keys = self.__meta_data.keys()

        for key in keys:
            md = self.__meta_data[key]

            dom_string = ""
            for i in key:
                dom_string += "_"
                dom_string += str(i)

            base_name = self.__name+dom_string
            var_group = safe_create_group(base, base_name)
            mesh_group = safe_create_group(var_group, "mesh")

            # Add mesh attributes.
          #  mesh_group.attrs["vsType"] = np.string_("mesh")
            kv.set_attrs(mesh_group.name, "vsType", "mesh");

            #mesh_group.attrs["vsKind"] = np.string_("rectilinear")
            kv.set_attrs(mesh_group.name, "vsKind", "rectilinear");
            #mesh_group.attrs["vsMD"] = np.string_(self.__name + "mesh")
            kv.set_attrs(mesh_group.name, "vsMD", self.__name + "mesh");

            data_shape = []

            if self.__type > 0:
                for i in range(self.__type):
                    data_shape.append(len(md))

            for i in range(len(md)):
                #mesh_group.attrs["vsAxis"+str(i)] = np.string_("axis"+str(i))
                kv.set_attrs(mesh_group.name, "vsAxis"+str(i), "axis"+str(i));
                ds = safe_create_dataset(mesh_group, "axis"+str(i), shape=(md[i].stop-md[i].start,), dtype='float64')
                data_shape.append(md[i].stop-md[i].start)

            # Compute shape for mesh datasets
            mesh_groups[key] = mesh_group
            test = safe_create_dataset(var_group, self.__name, shape=tuple(data_shape), dtype='float64')

            #test.attrs["vsType"] = np.string_('variable')
            kv.set_attrs(test.name, "vsType", "variable");
            #test.attrs["vsMesh"] = np.string_('/'+base_name+'/mesh')
            kv.set_attrs(test.name, "vsMesh", '/'+base_name+'/mesh')
            if self.__var_unit is not None:
                #test.attrs["vsUnits"] = np.string_(self.__var_unit)
                kv.set_attrs(test.name, "vsUnits", self.__var_unit);

            if self.__type > 0:

                #test.attrs["vsIndexOrder"] = np.string_("compMajorC")
                kv.set_attrs(test.name, "vsIndexOrder", "compMajorC");

                if self.__type < 2:
                    derived_vars = safe_create_group(base, "derived_vars")
                    #derived_vars.attrs["vsType"] = np.string_("vsVars")
                    kv.set_attrs(derived_vars.name, "vsType", "vsVars");

                    tmp = "{"
                    for i in range(len(md)):
                        tmp += self.__name+"_"+str(i)
                        if i < len(md)**self.__type-1:
                            tmp += ", "
                    tmp += "}"
                    #derived_vars.attrs[self.__name] = np.string_(tmp)
                    kv.set_attrs(derived_vars.name, self.__name, tmp)

            if 'time' in base:
                #test.attrs["vsTimeGroup"] = np.string_("/time")
                kv.set_attrs(test.name, "vsTimeGroup", "/time");

            #test.attrs["vsMD"] = np.string_(self.__name)
            kv.set_attrs(test.name, "vsMD", self.__name);
            datasets[key] = test

        # Create Mesh Groups/Datasets
        # Create Variable Datasets

        # This is done from data and domain.mesh for the corresponding process:
        # Write mesh and data
        for key in self.__data.keys():
            data_shape = []
            for i in range(len(key)):

                if H5P_COLLECTIVE:
                    with mesh_groups[key]["axis"+str(i)].collective:
                        mesh_groups[key]["axis"+str(i)][:] = self.__domains[key].mesh.axes[i][:]
                else:
                    mesh_groups[key]["axis"+str(i)][:] = self.__domains[key].mesh.axes[i][:]

            if H5P_COLLECTIVE:
                with datasets[key].collective:
                    datasets[key][:] = self.__data[key][:]
            else:
                datasets[key][:] = self.__data[key][:]
        if hasattr(context, 'comm'):
            context.comm.Barrier()

        # delete groups and datasets before closing the file!
        for d in datasets.values():
            try:
                del d
            except UnboundLocalError:
                pass

        try:
            del datasets
        except UnboundLocalError:
            pass

        try:
            del var_group
        except UnboundLocalError:
            pass

        try:
            del mesh_group
        except UnboundLocalError:
            pass

        for g in mesh_groups.values():
            try:
                del g
            except UnboundLocalError:
                pass

        try:
            del mesh_groups
        except UnboundLocalError:
            pass
