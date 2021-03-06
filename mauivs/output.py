# -*- coding: utf-8 -*-

from __future__ import division

__author__ = 'christoph.statz <at> tu-dresden.de'

import maui
import sys
import socket
import datetime
import getpass

from maui.mesh import RectilinearMesh
from maui.field import ScalarField, VectorField, TensorField
from .writer import VSWriter
from .helper import *
from os import makedirs
import h5py as h5
import numpy as np

from maui.backend import context

import kv 

def write_time(base, cycle, time):

	if cycle is not None:

		time_group = safe_create_group(base, "time")
		kv.set_attrs(time_group.name, "vsType", "time");
	#	time_group.attrs["vsType"] = np.string_("time")

		if cycle is not None:
			kv.set_attrs(time_group.name, "vsStep", cycle);
	#		time_group.attrs["vsStep"] = cycle
		if time is not None:
			kv.set_attrs(time_group.name, "vsTime", time);
	#		time_group.attrs["vsTime"] = time

		del time_group


def write_provenance_data(h5file):

        run_info = safe_create_group(h5file, "runinfo")
        #run_info.attrs["vsType"] = np.string_("runInfo")
	kv.set_attrs(run_info.name, "vsType", "runInfo");
        #run_info.attrs["vsSoftware"] = np.string_("maui")
	kv.set_attrs(run_info.name, "vsSoftware", "maui");
        #run_info.attrs["vsSwVersion"] = np.string_(maui.__version__)
	kv.set_attrs(run_info.name, "vsSwVersion", maui.__version__);
	#run_info.attrs["vsVsVersion"] = np.string_("4.0.0")
	kv.set_attrs(run_info.name, "vsVsVersion", "4.0.0");
        #run_info.attrs["vsUser"] = np.string_(getpass.getuser())
	kv.set_attrs(run_info.name, "vsUser", getpass.getuser());
        #run_info.attrs["vsRunDate"] = np.string_(str(datetime.datetime.now()))
	kv.set_attrs(run_info.name, "vsRunDate", str(datetime.datetime.now()))

        tmp = str(sys.executable) + " "
        for arg in sys.argv:
            tmp = tmp + str(arg) + " "

        #run_info.attrs["vsCommandLine"] = np.string_(tmp.strip())
	kv.set_attrs(run_info.name, "vsCommandLine", tmp.strip())
        #run_info.attrs["vsRunHost"] = np.string_(socket.gethostname())
	kv.set_attrs(run_info.name, "vsRunHost", socket.gethostname())

        del run_info


class VSOutput(object):

    def __init__(self, fields, identifier, prefix='.', local_only=False, omit_if_possible=False):
        """ Vizschema (VS) output object.

        :param fields: List of Field or Field, data to be visualized.
        :param identifier: String, unique name/basename out the output files.
        :param prefix: String, directory prefix (where the files go).
        """


        self.__local_only = local_only

        if isinstance(fields, TensorField) or isinstance(fields, VectorField) or isinstance(fields, ScalarField):
            self.__fields = [fields]
        elif type(fields) is list:
            self.__fields = fields
        elif type(fields) is tuple:
            self.__fields = list(fields)
        else:
            raise TypeError("Unsupported data type.")

        self.__omit = True and omit_if_possible

        for f in self.__fields:
            #print f.d.keys()
            if len(f.d.keys()) > 0:
                self.__omit = False

        for field in self.__fields:
            if not isinstance(field.partition.mesh, RectilinearMesh):
                raise TypeError("Currently only fields associated with a rectilinear mesh are supported: %s !" % field.name)

        self.__identifier = identifier
        self.__prefix = prefix + "/"

        names = [field.name for field in self.__fields]

        if len(set(names)) != len(names):
            raise ValueError("Fields need to have a unique identifier!")

        try:
            makedirs(prefix)
        except OSError:
            pass

        self.__writer = []

        for field in self.__fields:
            if not (self.__omit and self.__local_only):
                self.__writer.append(VSWriter(field.partition.domains, field.d, field.partition.meta_data,
                    var_name=field.name, var_type=field.rank, var_unit=field.unit, local_only=self.__local_only))

        self.__cycle = 0

        if self.__local_only and hasattr(context, 'comm'):

            self.__prefix = self.__prefix + str(context.rank) + '/'

            if not self.__omit:
                try:
                    makedirs(self.__prefix)
                except OSError:
                    pass

    def write(self, cycle=None, time=None):
        """ Routine to invoke the datafile genration.

        :param cycle: int cycle
        :param time: float time
        """

        # TODO: iterate through fields and check if the process needs todo io!

        filename = self.__prefix+self.__identifier

        if cycle is None and time is not None:
            cycle = self.__cycle
            self.__cycle += 1

        if cycle is not None:
            filename += "_%07d" % cycle

        base = None

        if hasattr(context, 'comm'):
            if not self.__local_only:
                base = h5.File(filename+'.vsh5', 'w', driver='mpio', comm=context.comm)
            else:
                if not self.__omit:
                    base = h5.File(filename+'.vsh5', 'w', driver='sec2')
        else:
            base = h5.File(filename+'.vsh5', 'w')

        base_name = '/'

        if not self.__omit:
            write_time(base, cycle, time)
            write_provenance_data(base)

            for writer in self.__writer:
                writer.write(base, base_name)
  
            base.close()
            del base
            if not hasattr(context, 'comm') or self.__local_only or context.rank is 0:
                kv.finalize(filename)
