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

import hashlib
import json
import logging
from graphscope.framework.graph import Graph
from graphscope.framework.dag_utils import bind_app
from graphscope.framework.context import create_context_node
from graphscope.framework.dag import DAGNode
from graphscope.framework.graph_schema import Property
from graphscope.framework.app import AppDAGNode
from graphscope.framework.app import load_app
import yaml
from graphscope.framework.app import AppAssets
from graphscope.framework.app import not_compatible_for
from graphscope.framework.app import project_to_simple
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
WORKSPACE = "/tmp/gs"
GRAPE_SDK_BUILD=os.path.join(str(Path.home()), "GAE-ODPSGraph/pie-sdk/grape-sdk/target/classes/")
VINEYARD_GRAPH_SDK_BUILD=os.path.join("/home/admin/GAE-ODPSGraph/pie-sdk/vineyard-graph/target/classes/")
GRAPE_JNI_LIB_PATH=os.path.join("/home/admin/GAE-ODPSGraph/pie-sdk/grape-sdk/target/native/")
VINEYARD_JNI_LIB_PATH=os.path.join("/home/admin/GAE-ODPSGraph/pie-sdk/vineyard-graph/target/native/")
FFI_M2_REPO_PATH=os.path.join(str(Path.home()), ".m2/repository/com/alibaba/ffi")
LLVM4JNI_JAR=os.path.join(FFI_M2_REPO_PATH, "llvm4jni-runtime/0.1/llvm4jni-runtime-0.1-jar-with-dependencies.jar")
# GUAVA_JAR=os.path.join(str(Path.home()), ".m2/repository/com/google/guava/guava/30.1.1-jre/guava-30.1.1-jre.jar")

class JavaApp(AppAssets):
    def __init__(self, full_jar_path : str, java_app_class: str, java_app_type : str):
        self._java_app_class = java_app_class
        self._full_jar_path = full_jar_path
        self._java_app_type = java_app_type
        gar = self._pack_jar(self._full_jar_path)
        #TODO: remove this
        java_main_class = "io.graphscope.example.TraverseMain"
        gs_config = {
            "app": [
                {
                    "algo": "java_app",
                    "type": "java_pie",
                    "java_main_class" : java_main_class,
                    "java_jar_path": self.jar_path,
                    "java_app_class": self.java_app_class
                }
            ]
        }
        #For two different java type, we use two different driver class
        if java_app_type == "property":
            self._cpp_driver_class =  "gs::JavaPIEPropertyDefaultApp"
            gs_config["app"][0]["class_name"] =self.cpp_driver_class
            gs_config["app"][0]["compatible_graph"] = ["vineyard::ArrowFragment"]
            gs_config["app"][0]["context_type"] = "java_pie_property_default_context"
            gar.append(DEFAULT_GS_CONFIG_FILE, yaml.dump(gs_config))
            super().__init__("java_app","java_pie_property_default_context",gar.read_bytes())
        elif java_app_type == "projected":
            self._cpp_driver_class = "gs::JavaPIEProjectedDefaultApp"
            gs_config["app"][0]["class_name"] = self.cpp_driver_class
            gs_config["app"][0]["compatible_graph"] = ["gs::ArrowProjectedFragment"]
            gs_config["app"][0]["context_type"] = "java_pie_projected_default_context"
            gar.append(DEFAULT_GS_CONFIG_FILE, yaml.dump(gs_config))
            super().__init__("java_app","java_pie_projected_default_context",gar.read_bytes())
        else:
            raise RuntimeError("Unexpected app type: {}".format(java_app_type))
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
    def _pack_jar(self, full_jar_path):
        garfile = InMemoryZip()
        tmp_jar_file = open(full_jar_path, 'rb')
        bytes = tmp_jar_file.read()
        garfile.append("{}".format(full_jar_path.split("/")[-1]), bytes)
        return garfile
    def signature(self):
        s = hashlib.sha256()
        s.update(f"{self.type}.{self.jar_path}.{self.cpp_driver_class}.{self.java_app_class}".encode("utf-8"))
        s.update(self.gar)
        return s.hexdigest()
    def __call__(self, graph : Graph, *args, **kwargs):
        kwargs_extend = dict(app_class = self.java_app_class, **kwargs)
        if not hasattr(graph, "graph_type"):
            raise InvalidArgumentError("Missing graph_type attribute in graph object.")
        if graph.graph_type == graph_def_pb2.ARROW_PROPERTY:
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
        # we can not determine the compiled lib path here, so we find all possible subdirectories,
        # and add them to java.library.path
        # possible_library_directories = [s.rstrip("/") for s in glob("{}/[!gs\-ffi]*/".format(udf_workspace))]
        # user_jar = [s.rstrip("/") for s in glob("{}/*/*.jar".format(udf_workspace))]

        user_jni_name = self._app_assets.signature()
        user_jni_dir = os.path.join(udf_workspace, user_jni_name)
        # Here we provide the full path to lib for server, because jvm is created for once,
        # when used at second time(app), we need to load jni library, but it will not be in 
        # the library.path. But loading with absolute path is possible.
        user_jni_name_lib = os.path.join(user_jni_dir, self.get_lib_path(user_jni_name))
        user_jar = os.path.join(user_jni_dir, self._app_assets.jar_path)
        assert (os.path.isfile(user_jni_name_lib)), "{} not found ".format(user_jni_name_lib)
        assert (os.path.isfile(user_jar)), "{} not found ".format(user_jar)

        logger.info("user jni library found: {}".format(user_jni_name_lib))
        logger.info("user jar found: {}".format(user_jar))
        ffi_target_output = os.path.join(udf_workspace, "gs-ffi-{}".format(user_jni_name), "CLASS_OUTPUT")
        performance_args = "-Dcom.alibaba.ffi.rvBuffer=2147483648 -XX:+StartAttachListener " \
                        + "-XX:+PreserveFramePointer -XX:+UseParallelGC -XX:+UseParallelOldGC " \
                        + "-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UnlockDiagnosticVMOptions -XX:LoopUnrollLimit=1"
        #grape jni and vineyard jni will be put in jar file, and extracte, add to path during runtime
        jvm_runtime_opt_impl = "-Xrs " \
                        + "-Djava.library.path=/usr/local/lib:/usr/lib:{}:{}:{} ".format(user_jni_dir, GRAPE_JNI_LIB_PATH, VINEYARD_JNI_LIB_PATH)\
                        + "-Djava.class.path={}:{}:{}:{}:{} {}"\
                         .format(ffi_target_output, GRAPE_SDK_BUILD, VINEYARD_GRAPH_SDK_BUILD, user_jar, LLVM4JNI_JAR, performance_args)
        logger.info("running {} with jvm options: {}".format(self._app_assets.algo, jvm_runtime_opt_impl))

        frag_name_for_java = ""
        if self._app_assets.java_app_type =="property":
            frag_name_for_java = self._convert_arrow_frag_for_java(self._graph.template_str)
            logger.info("Set frag name to {}, {}".format(self._graph.template_str, frag_name_for_java))
        else :
            frag_name_for_java = self._graph.template_str
        kwargs_extend = dict(
            jvm_runtime_opt=jvm_runtime_opt_impl,
            user_library_name = user_jni_name_lib,
            frag_name = frag_name_for_java,
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
