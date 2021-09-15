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
import os
from glob import glob
from pathlib import Path
__all__ = ["JavaAppAssets"]

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
    def __init__(self, jar_path : str, app_class: str, app_type : str):

        garfile = InMemoryZip()
        tmp_jar_file = open(jar_path, 'rb')
        bytes = tmp_jar_file.read()
        garfile.append("{}".format(jar_path.split("/")[-1]), bytes)
        self.java_jar_path_full_ = jar_path
        
        #TODO: remove this
        java_main_class = "io.graphscope.example.TraverseMain"
        if app_type == "property":
            self.app_class_ = "gs::JavaPIEPropertyDefaultApp"
        else if app_type == "projected":
            self.app_class_ = "gs::"
        gs_config = {
            "app": [
                {
                    "algo": "java_app_assets",
                    "context_type": "java_pie_property_default_context",
                    "type": "java_pie",
                    "class_name": self.app_class,
                    "compatible_graph": ["vineyard::ArrowFragment", "vineyard::ArrowProjectedFragment"],
                    "java_main_class" : java_main_class,
                    "java_jar_path": self.java_jar_path
                }
            ]
        }
        garfile.append(DEFAULT_GS_CONFIG_FILE, yaml.dump(gs_config))
        super().__init__("java_app_assets","java_pie_property_default_context",garfile.read_bytes())