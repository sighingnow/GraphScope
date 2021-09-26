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
from graphscope.analytical.udf.utils import CType
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
LLVM4JNI_SDK_BASE = "sdk-llvm4jni-output"
LLVM4JNI_USER_BASE = "user-llvm4jni-output"
WORKSPACE = "/tmp/gs"
#TODO make this not fixed, 
GRAPE_SDK_BUILD=os.path.join(str(Path.home()), "GAE-ODPSGraph/pie-sdk/grape-sdk/target/classes/")
VINEYARD_GRAPH_SDK_BUILD=os.path.join("/home/admin/GAE-ODPSGraph/pie-sdk/vineyard-graph/target/classes/")
GRAPE_JNI_LIB_PATH=os.path.join("/home/admin/GAE-ODPSGraph/pie-sdk/grape-sdk/target/native/")
VINEYARD_JNI_LIB_PATH=os.path.join("/home/admin/GAE-ODPSGraph/pie-sdk/vineyard-graph/target/native/")
FFI_M2_REPO_PATH=os.path.join(str(Path.home()), ".m2/repository/com/alibaba/ffi")
LLVM4JNI_JAR=os.path.join(FFI_M2_REPO_PATH, "llvm4jni-runtime/0.1/llvm4jni-runtime-0.1-jar-with-dependencies.jar")
# GUAVA_JAR=os.path.join(str(Path.home()), ".m2/repository/com/google/guava/guava/30.1.1-jre/guava-30.1.1-jre.jar")

class JavaApp(AppAssets):
    def __init__(self, full_jar_path : str, java_app_class: str):
        self._java_app_class = java_app_class
        self._full_jar_path = full_jar_path
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
        self._java_app_type, self._frag_param_str = self._parse_user_app(java_app_class, full_jar_path)
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
    def java_app_type(self):
        return self._java_app_type
    @property
    def frag_param_str(self):
        return self._frag_param_str
    def _pack_jar(self, full_jar_path):
        garfile = InMemoryZip()
        tmp_jar_file = open(full_jar_path, 'rb')
        bytes = tmp_jar_file.read()
        garfile.append("{}".format(full_jar_path.split("/")[-1]), bytes)
        return garfile
    def _judge_graph_app_consistency(self, graph_template_str : str, java_app_type : str, frag_param_str: str):
        splited = graph_template_str.split("<")
        java_app_type_params = frag_param_str.split(",")
        if len(splited != 2):
            raise Exception("Unrecoginizable graph template str: {}".format(graph_template_str))
        if (splited[0] == "vineyard::ArrowFragment"):
            if (java_app_type != "property"):
                return False
        if (splited[1] == "gs::ArrowProjectedFragment"):
            if (java_app_type != "projected"):
                return False
        graph_actual_type_params = splited[1][:-1].split(",")
        if (len(graph_actual_type_params) != len(java_app_type_params)):
            raise Exception("Although graph type same, the type params can not match")
        for graph_actucal_type_param, java_app_type_param in zip(graph_actual_type_params, java_app_type_params):
            if (not self._type_param_consistent(graph_actucal_type_param, java_app_type_param)):
                return False
        return True
    # Override is_compatible to make sure type params of graph consists with java app.
    def is_compatible(self, graph):
        return self._judge_graph_app_consistency(graph.template_str, self.java_app_type, self.frag_param_str)
    def signature(self):
        s = hashlib.sha256()
        s.update(f"{self.type}.{self.jar_path}.{self.cpp_driver_class}.{self.java_app_class}".encode("utf-8"))
        s.update(self.gar)
        return s.hexdigest()
    def _parse_user_app(self, java_app_class: str, java_jar_full_path : str):
        _java_app_type = ""
        _frag_param_str = ""
        parse_user_app_cmd = [
            "java",
            "-cp",
            "{}".format(java_jar_full_path),
            "io.v6d.modules.graph.utils.AppBaseParser",
            java_app_class,
        ]
        java_env=os.environ.copy()
        logger.info(" ".join(parse_user_app_cmd))
        parse_user_app_process = subprocess.Popen(
            parse_user_app_cmd,
            env=java_env,
            universal_newlines=True,
            encoding="utf-8",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        for line in parse_user_app_process.stdout:
            logger.info(line)
            if line.find("PropertyDefaultApp"):
                _java_app_type = "property"
            if line.find("ProjectedDefaultApp"):
                _java_app_type = "projected"
            if line.find("Error"):
                raise Exception("Error occured in verifying user app")
            if line.find("TypeParams"):
                _frag_param_str = line.split(":")[1].strip()
        logger.info("java app type: {}, frag type str: {}]".format(_java_app_type, _frag_param_str))

        #java_codegen_stderr_watcher = PipeWatcher(parse_user_app_process.stderr, sys.stdout)
        #setattr(parse_user_app_process, "stderr_watcher", java_codegen_stderr_watcher)
        parse_user_app_process.wait()
        return _java_app_type,_frag_param_str
    def __call__(self, graph : Graph, *args, **kwargs):
        kwargs_extend = dict(app_class = self.java_app_class, **kwargs)
        if not hasattr(graph, "graph_type"):
            raise InvalidArgumentError("Missing graph_type attribute in graph object.")
        # Check the consistency between Java app and applied graph

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

        """Convert ArrowFragment<O,V> to ArrowFragmentDefault<O>"""
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
        # set the jvm_opts as a kw
        jvm_runtime_opt_impl = ""
        udf_workspace = os.path.join(WORKSPACE, self._session.session_id)

        user_jni_name = self._app_assets.signature()
        user_jni_dir = os.path.join(udf_workspace, user_jni_name)
        # Here we provide the full path to lib for server, because jvm is created for once,
        # when used at second time(app), we need to load jni library, but it will not be in 
        # the library.path. But loading with absolute path is possible.
        user_jni_name_lib = os.path.join(user_jni_dir, self.get_lib_path(user_jni_name))
        user_jar = os.path.join(user_jni_dir, self._app_assets.jar_path)
        assert (os.path.isfile(user_jni_name_lib)), "{} not found ".format(user_jni_name_lib)
        assert (os.path.isfile(user_jar)), "{} not found ".format(user_jar)

        LLVM4JNI_USER = os.path.join(udf_workspace, "{}-{}".format(LLVM4JNI_USER_BASE, user_jni_name))
        LLVM4JNI_SDK = os.path.join(udf_workspace, LLVM4JNI_SDK_BASE)

        logger.info("user jni library found: {}".format(user_jni_name_lib))
        logger.info("user jar found: {}".format(user_jar))
        ffi_target_output = os.path.join(udf_workspace, "gs-ffi-{}".format(user_jni_name), "CLASS_OUTPUT")
        #TODO: Params as file, configuration 哪些需要配置，哪些不需要
        performance_args = "-Dcom.alibaba.ffi.rvBuffer=2147483648 -XX:+StartAttachListener " \
                        + "-XX:+PreserveFramePointer -XX:+UseParallelGC -XX:+UseParallelOldGC " \
                        + "-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UnlockDiagnosticVMOptions -XX:LoopUnrollLimit=1"
        #grape jni and vineyard jni will be put in jar file, and extracte, add to path during runtime
        jvm_runtime_opt_impl = "-Xrs " \
                        + "-Djava.library.path=/usr/local/lib:/usr/lib:{}:{}:{} ".format(user_jni_dir, GRAPE_JNI_LIB_PATH, VINEYARD_JNI_LIB_PATH)\
                        + "-Djava.class.path={}:{}:{}:{}:{}:{}:{} {}"\
                         .format(ffi_target_output,LLVM4JNI_SDK, LLVM4JNI_USER, GRAPE_SDK_BUILD, VINEYARD_GRAPH_SDK_BUILD, user_jar, LLVM4JNI_JAR, performance_args)
        # TODO: remove GRAPE_SDK_BUILD and VINEYARD_SDK_BUILD when jar build problem is solved.
        logger.info("running {} with jvm options: {}".format(self._app_assets.algo, jvm_runtime_opt_impl))

        frag_name_for_java = ""
        if self._app_assets.java_app_type =="property":
            frag_name_for_java = self._convert_arrow_frag_for_java(self._graph.template_str)
            logger.info("Set frag name to {}, {}".format(self._graph.template_str, frag_name_for_java))
        else :
            frag_name_for_java = self._graph.template_str 
        # get number of worker on each host, so we can determine the java memory settings.
        sess_info_ = self._session.info
        num_hosts_ = len(sess_info_["engine_hosts"].split(","))
        num_worker_ = int(sess_info_["num_workers"])
        kwargs_extend = dict(
            jvm_runtime_opt=jvm_runtime_opt_impl,
            user_library_name = user_jni_name_lib,
            frag_name = frag_name_for_java,
            num_hosts= num_hosts_,
            num_worker = num_worker_,
            **kwargs
        )


        logger.info("dumping to json {}".format(json.dumps(kwargs_extend)))
        return create_context_node(context_type, self, self._graph, json.dumps(kwargs_extend))
    def get_lib_path(self, app_name):
        lib_path = ""
        if sys.platform == "linux" or sys.platform == "linux2":
            lib_path = "lib%s.so" % app_name
        elif sys.platform == "darwin":
            lib_path = "lib%s.dylib" % app_name
        else:
            raise RuntimeError("Unsupported platform.")
        return lib_path
