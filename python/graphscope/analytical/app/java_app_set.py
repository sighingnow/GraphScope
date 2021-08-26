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


# from coordinator.gscoordinator.coordinator import DEFAULT_GS_CONFIG_FILE


import copy
import hashlib
import json
import logging
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
import os
from glob import glob
from pathlib import Path
__all__ = ["JavaAppAssets"]

logger = logging.getLogger("graphscope")

DEFAULT_GS_CONFIG_FILE = ".gs_conf.yaml"
WORKSPACE = "/tmp/gs"
GRAPE_M2_REPO_PATH = os.path.join(str(Path.home()), ".m2/repository/com/alibaba/grape")
# VINEYARD_GRAPH_REPO_PATH = os.path.join(str(Path.home()), "./m2/repository/io/v6d")
GRAPE_PROCESSOR_JAR=os.path.join(GRAPE_M2_REPO_PATH, "grape-processor/0.1/grape-processor-0.1-jar-with-dependencies.jar")
GRAPE_SDK_JAR=os.path.join(GRAPE_M2_REPO_PATH, "grape-sdk/0.1/grape-sdk-0.1-jar-with-dependencies.jar")
GRAPE_JNI_LIB_PATH=os.path.join("/home/admin/GAE-ODPSGraph/pie-sdk/grape-sdk/target/native/")
VINEYARD_GRAPH_SDK_JAR = os.path.join(GRAPE_M2_REPO_PATH, "vineyard-graph/0.1/vineyard-graph-0.1.jar-with-dependencies.jar")
VINEYARD_JNI_LIB_PATH=os.path.join("/home/admin/GAE-ODPSGraph/pie-sdk/vineyard-graph/target/native/")
FFI_M2_REPO_PATH=os.path.join(str(Path.home()), ".m2/repository/com/alibaba/ffi")
LLVM4JNI_JAR=os.path.join(FFI_M2_REPO_PATH, "llvm4jni-runtime/0.1/llvm4jni-runtime-0.1-jar-with-dependencies.jar")
GUAVA_JAR=os.path.join(str(Path.home()), ".m2/repository/com/google/guava/guava/30.1.1-jre/guava-30.1.1-jre.jar")

class JavaAppAssets(AppAssets):
    """Wrapper for a java jar, containing some java apps

    Args:
        graph (:class:`Graph`): A projected simple graph.
        java_main_class :java main class to run before codegen, geneerating 
                         configurations.
        vd_type: int64, uint64
        md_type: int64, uint64

    Returns:
        :class:`graphscope.framework.context.VertexDataContextDAGNode`:

    Examples:

    .. code:: python

        import graphscope as gs
        sess = gs.session()
        g = sess.g()
        pg = g.project(vertices={"vlabel": []}, edges={"elabel": []})
        r = gs.java_app_set(pg, jar_path = "a.jar", java_main_class="com.aliababa.grape.sample.mainClass")
        s.close()

    """
    def __init__(self, jar_path : str , java_main_class : str, vd_type, md_type):
        vd_ctype = str(CType.from_string(vd_type)) # _t appended
        md_ctype = str(CType.from_string(md_type))
        garfile = InMemoryZip()
        tmp_jar_file = open(jar_path, 'rb')
        bytes = tmp_jar_file.read()
        garfile.append("{}".format(jar_path.split("/")[-1]), bytes)
        self.java_jar_path_ = jar_path.split("/")[-1]
        self.java_main_class_ = java_main_class
        self.app_class_ = "grape::JavaPIEPropertyDefaultApp"
        self.vd_type_ = vd_type
        self.md_type_ = md_type
        gs_config = {
            "app": [
                {
                    "algo": "java_app_set",
                    "context_type": "java_pie_property_default_context",
                    "type": "java_pie",
                    "class_name": self.app_class ,
                    "compatible_graph": ["vineyard::ArrowFragment"],
                    "vd_type": vd_ctype,
                    "md_type": md_ctype,
                    "java_main_class" : self.java_main_class,
                    "java_jar_path": self.java_jar_path
                }
            ]
        }
        garfile.append(DEFAULT_GS_CONFIG_FILE, yaml.dump(gs_config))
        super().__init__("java_app_set","java_pie_property_default_context",garfile.read_bytes())
    def to_gar(self, path):
        if os.path.exists(path):
            raise RuntimeError("Path exist: {}.".format(path))
        with open(path, "wb") as f:
            f.write(self.gar)
    def __call__(self, graph, *args, **kwargs):
        app_ = graph.session._wrapper(JavaAppDagNode(graph, self))
        return app_(*args, **kwargs)
    @property
    def java_jar_path(self):
        return self.java_jar_path_
    @property
    def frag_name(self):
        return  "{}<{},{}>".format(
            "vineyard::ArrowFragment",
            "int64_t",
            "uint64_t",
        )
    @property
    def java_main_class(self):
        return self.java_main_class_
    @property
    def app_class(self):
        return self.app_class_
    @property
    def vd_type(self):
        return self.vd_type_
    @property
    def md_type(self):
        return self.md_type_

    #shall be the same as defined in coordinator/utils.py
    def signature(self):
        s = hashlib.sha256()
        s.update(f"{self.type}.{self.app_class}.{self.frag_name}.{self.java_jar_path}.{self.java_main_class}".encode("utf-8"))
        s.update(self.gar)
        return s.hexdigest()


class JavaAppDagNode(AppDAGNode):
    """retrict appassets to javaAppAssets"""
    def __init__(self, graph, app_assets: JavaAppAssets):
        """Create an application using given :code:`gar` file, or given application
            class name.

        Args:
            graph (:class:`GraphDAGNode`): A :class:`GraphDAGNode` instance.
            app_assets: A :class:`AppAssets` instance.
        """
        self._graph = graph
        "add deep copy to allow user run app for multiple times on a same javaAppAssets"
        "the ops are new created, but built library should be reused"
        # self._app_assets = copy.deepcopy(app_assets)
        self._app_assets = JavaAppAssets(app_assets.java_jar_path, app_assets.java_main_class, app_assets.vd_type, app_assets.md_type)
        self._session = graph.session
        self._app_assets.is_compatible(self._graph)

        self._op = bind_app(graph, self._app_assets)
        # add op to dag
        self._session.dag.add_op(self._app_assets.op)
        self._session.dag.add_op(self._op)

    def __call__(self, *args, **kwargs):
        """When called, check arguments based on app type, Then do build and query.

        Raises:
            InvalidArgumentError: If app_type is None,
                or positional argument found when app_type not `cpp_pie`.

        Returns:
            :class:`Context`: Query context, include running results of the app.
        """
        app_type = self._app_assets.type
        check_argument(app_type == "java_pie", "expect java_pie app")
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
        user_jni_name_lib = os.path.join(user_jni_dir, "lib{}.so".format(user_jni_name))
        user_jar = os.path.join(user_jni_dir, self._app_assets.java_jar_path)
        assert (os.path.isfile(user_jni_name_lib)), "{} not found ".format(user_jni_name_lib)
        assert (os.path.isfile(user_jar)), "{} not found ".format(user_jar)

        logger.info("user jni library found: {}".format(user_jni_name_lib))
        logger.info("user jar found: {}".format(user_jar))
        ffi_target_output = os.path.join(udf_workspace, "gs-ffi", "CLASS_OUTPUT")
        performance_args = "-Dcom.alibaba.ffi.rvBuffer=2147483648 -XX:+StartAttachListener " \
                        + "-XX:+PreserveFramePointer -XX:+UseParallelGC -XX:+UseParallelOldGC " \
                        + "-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UnlockDiagnosticVMOptions -XX:LoopUnrollLimit=1"
        #grape jni and vineyard jni will be put in jar file, and extracte, add to path during runtime
        jvm_runtime_opt_impl = "-Djava.library.path=/usr/local/lib:/usr/lib:{}:{}:{} ".format(user_jni_dir, GRAPE_JNI_LIB_PATH, VINEYARD_JNI_LIB_PATH)\
                        + "-Djava.class.path={}:{}:{}:{}:{}:{} {}"\
                         .format(ffi_target_output,  GUAVA_JAR, GRAPE_SDK_JAR, VINEYARD_GRAPH_SDK_JAR, user_jar, LLVM4JNI_JAR, performance_args)
        logger.info("running {} with jvm options: {}".format(self._app_assets.algo, jvm_runtime_opt_impl))
        kwargs_extend = dict(jvm_runtime_opt=jvm_runtime_opt_impl, frag_name = self._app_assets.frag_name, **kwargs)
        # just set the jni library name (without lib prefix, and also no path)
        kwargs_extend = dict(user_library_name = user_jni_name,  **kwargs_extend)
        logger.info("dumping to json {}".format(json.dumps(kwargs_extend)))
        return create_context_node(context_type, self, self._graph, json.dumps(kwargs_extend))


