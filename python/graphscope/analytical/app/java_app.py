#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# from coordinator.gscoordinator.io_utils import PipeWatcher
import hashlib
import json
import logging
import subprocess
from graphscope.framework.graph import Graph
from graphscope.framework.dag_utils import bind_app
from graphscope.framework.context import create_context_node
from graphscope.framework.dag import DAGNode
from graphscope.framework.app import AppDAGNode
import yaml
from graphscope.framework.app import AppAssets
from graphscope.analytical.udf.utils import InMemoryZip
from graphscope.framework.app import check_argument
from graphscope.framework.errors import InvalidArgumentError
from graphscope.proto import graph_def_pb2
import os
from glob import glob
from pathlib import Path
import sys
__all__ = ["JavaApp"]

logger = logging.getLogger("graphscope")

DEFAULT_GS_CONFIG_FILE = ".gs_conf.yaml"
LLVM4JNI_USER_OUT_DIR_BASE = "user-llvm4jni-output"

JAVA_CODEGNE_OUTPUT_PREFIX = 'gs-ffi'
WORKSPACE = "/tmp/gs"
GRAPHSCOPE_JAVA_HOME = None
if "GRAPHSCOPE_JAVA_HOME" not in os.environ:
    # only launch GAE
    logger.error("Can't found GRAPHSCOPE_JAVA_HOME in environment.")
else:
    GRAPHSCOPE_JAVA_HOME = os.environ["GRAPHSCOPE_JAVA_HOME"]
#In future, this two lib will be move to GRAPHSCOPE_HOME/lib
GRAPE_JNI_LIB=os.path.join(GRAPHSCOPE_JAVA_HOME,  "grape-sdk/target/native")
GRAPHSCOPE_JNI_LIB = os.path.join(GRAPHSCOPE_JAVA_HOME,  "graphscope-sdk/target/native")
DEFAULT_JVM_CONFIG_FILE = os.path.join(GRAPHSCOPE_JAVA_HOME, "jvm_options.yaml")
GRAPHSCOPE_RUNTIME_JAR=os.path.join(GRAPHSCOPE_JAVA_HOME, "graphscope-runtime/target/graphscope-runtime-0.1.jar")
def _parse_user_app(java_app_class: str, java_jar_full_path : str):
    _java_app_type = ""
    _frag_param_str = ""
    parse_user_app_cmd = [
        "java",
        "-cp",
        "{}".format(java_jar_full_path),
        "io.graphscope.utils.AppBaseParser",
        java_app_class,
    ]
    logger.info(" ".join(parse_user_app_cmd))
    parse_user_app_process = subprocess.Popen(
        parse_user_app_cmd,
        env=os.environ.copy(),
        universal_newlines=True,
        encoding="utf-8",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    out,err = parse_user_app_process.communicate()
    for line in err:
        logger.error(line)
    for line in out.split("\n"):
        logger.info(line)
        if (len(line) == 0):
            continue
        if line.find("PropertyDefaultApp") != -1:
            _java_app_type = "property"
            continue
        if line.find("ProjectedDefaultApp") != -1:
            _java_app_type = "projected"
            continue
        if line.find("Error") != -1:
            raise Exception("Error occured in verifying user app")
        if line.find("TypeParams") != 1:
            _frag_param_str = line.split(":")[1].strip()
    logger.info("Java app type: {}, frag type str: {}]".format(_java_app_type, _frag_param_str))

    #java_codegen_stderr_watcher = PipeWatcher(parse_user_app_process.stderr, sys.stdout)
    #setattr(parse_user_app_process, "stderr_watcher", java_codegen_stderr_watcher)
    parse_user_app_process.wait()
    return _java_app_type,_frag_param_str

def _type_param_consistent(graph_actucal_type_param, java_app_type_param):
    if (java_app_type_param == "java.lang.Long"):
        if (graph_actucal_type_param in {"uint64_t", "int64_t"}):
            return True
        return False
    if (java_app_type_param == "java.lang.Double"):
        if (graph_actucal_type_param in {"double"}):
            return True
        return False
    if (java_app_type_param == "java.lang.Integer"):
        if (graph_actucal_type_param in {"int32_t", "uint32_t"}):
            return True
        return False
    return False

def _get_lib_path(app_name):
    lib_path = ""
    if sys.platform == "linux" or sys.platform == "linux2":
        lib_path = "lib%s.so" % app_name
    elif sys.platform == "darwin":
        lib_path = "lib%s.dylib" % app_name
    else:
        raise RuntimeError("Unsupported platform.")
    return lib_path

# Java codegen cp can be empty when no bitcode is generated.
def _construct_jvm_options_from_params(app_lib_dir, jar_unpacked_path, llvm4jni_output_dir, java_codegen_cp):
    performance_args = " "
    #May be we should move this out of here.
    if os.path.isfile(DEFAULT_JVM_CONFIG_FILE):
        with open(DEFAULT_JVM_CONFIG_FILE, "r") as stream:
            config = yaml.safe_load(stream)
            performance_args = config["jvm"]["performance"]
    else:
        logger.info("No jvm config found, we still proceed...")
    
    library_path = "-Djava.library.path={}:{}".format(GRAPE_JNI_LIB, GRAPHSCOPE_JNI_LIB)
    runtime_class_path = "-Djava.class.path={}".format(GRAPHSCOPE_RUNTIME_JAR)
    #Put jni lib in user_class_path, so url class loader can find.
    user_class_path = "{}:{}:{}:{}:{}:{}".format(app_lib_dir, GRAPE_JNI_LIB, GRAPHSCOPE_JNI_LIB, llvm4jni_output_dir, java_codegen_cp, jar_unpacked_path)
    return " ".join([performance_args, library_path, runtime_class_path, "-Xrs"]).strip(), user_class_path

class JavaApp(AppAssets):
    def __init__(self, full_jar_path : str, java_app_class: str):
        self._java_app_class = java_app_class
        self._full_jar_path = full_jar_path
        self._jar_name = Path(self._full_jar_path).name
        gar = self._pack_jar(self._full_jar_path)
        gs_config = {
            "app": [
                {
                    "algo": "java_app",
                    "type": "java_pie",
                    "java_jar_path": self.jar_path,
                    "java_app_class": self.java_app_class
                }
            ]
        }
        #extract java app type with help of java class.
        self._java_app_type, self._frag_param_str = _parse_user_app(java_app_class, full_jar_path)
        #For two different java type, we use two different driver class
        if self._java_app_type == "property":
            self._cpp_driver_class =  "gs::JavaPIEPropertyDefaultApp"
            gs_config["app"][0]["driver_header"] = "apps/java_pie/java_pie_property_default_app.h"
            gs_config["app"][0]["class_name"] =self.cpp_driver_class
            gs_config["app"][0]["compatible_graph"] = ["vineyard::ArrowFragment"]
            gs_config["app"][0]["context_type"] = "java_pie_property_default_context"
            gar.append(DEFAULT_GS_CONFIG_FILE, yaml.dump(gs_config))
            super().__init__("java_app","java_pie_property_default_context",gar.read_bytes())
        elif self._java_app_type == "projected":
            self._cpp_driver_class = "gs::JavaPIEProjectedDefaultApp"
            gs_config["app"][0]["driver_header"] = "apps/java_pie/java_pie_projected_default_app.h"
            gs_config["app"][0]["class_name"] = self.cpp_driver_class
            gs_config["app"][0]["compatible_graph"] = ["gs::ArrowProjectedFragment"]
            gs_config["app"][0]["context_type"] = "java_pie_projected_default_context"
            gar.append(DEFAULT_GS_CONFIG_FILE, yaml.dump(gs_config))
            super().__init__("java_app","java_pie_projected_default_context",gar.read_bytes())
        else:
            raise RuntimeError("Unexpected app type: {}".format(self._java_app_type))
    # Override is_compatible to make sure type params of graph consists with java app.
    def is_compatible(self, graph):
        splited = graph.template_str.split("<")
        java_app_type_params = self.frag_param_str.split(",")
        num_type_params=0
        if len(splited) != 2:
            raise Exception("Unrecoginizable graph template str: {}".format(graph.template_str))
        if (splited[0] == "vineyard::ArrowFragment"):
            if (self.java_app_type != "property"):
                logger.error("Expected property app")
                return False
            if (len(java_app_type_params) != 1):
                logger.error("Expected one type params.")
                return False
            num_type_params=1
        if (splited[1] == "gs::ArrowProjectedFragment"):
            if (self.java_app_type != "projected"):
                logger.error("Expected projected app")
                return False
            if (len(java_app_type_params) != 4):
                logger.error("Expected 4 type params")
                return False
            num_type_params=4
        graph_actual_type_params = splited[1][:-1].split(",")
        for i in range(0, num_type_params):
            graph_actual_type_param = graph_actual_type_params[i]
            java_app_type_param = java_app_type_params[i]
            if (not _type_param_consistent(graph_actual_type_param, java_app_type_param)):
                return False
        return True
    def _pack_jar(self, full_jar_path):
        garfile = InMemoryZip()
        tmp_jar_file = open(full_jar_path, 'rb')
        bytes = tmp_jar_file.read()
        garfile.append("{}".format(full_jar_path.split("/")[-1]), bytes)
        return garfile

    def signature(self):
        s = hashlib.sha256()
        s.update(f"{self.type}.{self.jar_path}.{self.java_app_class}".encode("utf-8"))
        s.update(self.gar)
        return s.hexdigest()
    @property
    def java_app_class(self):
        return self._java_app_class
    @property
    def cpp_driver_class(self):
        return self._cpp_driver_class
    @property
    def jar_path(self):
        return self._full_jar_path
    @property
    def jar_name(self):
        return self._jar_name
    @property
    def java_app_type(self):
        return self._java_app_type
    @property
    def frag_param_str(self):
        return self._frag_param_str

    def __call__(self, graph : Graph, *args, **kwargs):
        kwargs_extend = dict(app_class = self.java_app_class, **kwargs)
        if not hasattr(graph, "graph_type"):
            raise InvalidArgumentError("Missing graph_type attribute in graph object.")

        if self.java_app_type == "projected" and graph.graph_type == graph_def_pb2.ARROW_PROPERTY:
            graph = graph._project_to_simple()
        app_ = graph.session._wrapper(JavaAppDagNode(graph, self))
        return app_(*args, **kwargs_extend)



class JavaAppDagNode(AppDAGNode):
    """retrict app assets to javaAppAssets"""
    def __init__(self, graph : Graph, app_assets: JavaApp):
        self._graph = graph
        self._app_assets = app_assets
        self._session = graph.session
        self._app_assets.is_compatible(self._graph)

        self._op = bind_app(graph, self._app_assets)
        # add op to dag
        self._session.dag.add_op(self._app_assets.op)
        self._session.dag.add_op(self._op)

        """Convert ArrowFragment<OID,VID> to ArrowFragmentDefault<OID>"""
    def _convert_arrow_frag_for_java(self, cpp_frag_str: str):
        res = cpp_frag_str.split(",")[0] + ">"
        return res.replace("<", "Default<", 1)

    def __call__(self, *args, **kwargs):
        """When called, check arguments based on app type, Then do build and query.

        Raises:
            InvalidArgumentError: If app_type is None,
                or positional argument found when app_type not `cpp_pie`.

        Returns:
            :class:`Context`: Query context, include running results of the app.
        """
        check_argument(self._app_assets.type == "java_pie", "expect java_pie app")
        context_type = self._app_assets.context_type

        if not isinstance(self._graph, DAGNode) and not self._graph.loaded():
            raise RuntimeError("The graph is not loaded")
        check_argument(
                not args, "Only support using keyword arguments in cython app."
            )
        udf_workspace = os.path.join(WORKSPACE, self._session.cluster_instance_id, self._session.session_id)

        app_lib_name = self._app_assets.signature()
        app_lib_dir = os.path.join(udf_workspace, app_lib_name)
        app_lib_full_path = os.path.join(app_lib_dir, _get_lib_path(app_lib_name))
        jar_unpacked_path = os.path.join(app_lib_dir, self._app_assets.jar_name)
        assert (os.path.isfile(app_lib_full_path)), "{} not found ".format(app_lib_full_path)
        assert (os.path.isfile(jar_unpacked_path)), "{} not found ".format(jar_unpacked_path)

        llvm4jni_output_dir = os.path.join(udf_workspace, "{}-{}".format(LLVM4JNI_USER_OUT_DIR_BASE, app_lib_name))
        #Java codegen directory can be empty.
        java_codegen_cp = os.path.join(udf_workspace, "{}-{}".format(JAVA_CODEGNE_OUTPUT_PREFIX, app_lib_name), "CLASS_OUTPUT")

        jvm_options,user_class_path = _construct_jvm_options_from_params(app_lib_dir, jar_unpacked_path, llvm4jni_output_dir, java_codegen_cp)

        logger.info("running {} with jvm options: {}, class path: {}".format(self._app_assets.algo, jvm_options, user_class_path))

        if self._app_assets.java_app_type =="property":
            frag_name_for_java = self._convert_arrow_frag_for_java(self._graph.template_str)
            logger.info("Set frag name to {}, {}".format(self._graph.template_str, frag_name_for_java))
        else :
            frag_name_for_java = self._graph.template_str 
        # get number of worker on each host, so we can determine the java memory settings.
        num_hosts_ = len(self._session.info["engine_hosts"].split(","))
        num_worker_ = int(self._session.info["num_workers"])
        kwargs_extend = dict(
            jvm_runtime_opt=jvm_options,
            user_class_path=user_class_path,
            user_library_name = app_lib_full_path,
            frag_name = frag_name_for_java,
            num_hosts= num_hosts_,
            num_worker = num_worker_,
            **kwargs
        )

        logger.info("dumping to json {}".format(json.dumps(kwargs_extend)))
        return create_context_node(context_type, self, self._graph, json.dumps(kwargs_extend))

