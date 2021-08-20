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


from coordinator.gscoordinator.coordinator import DEFAULT_GS_CONFIG_FILE
from graphscope.framework.app import load_app
import yaml
from graphscope.framework.app import AppAssets
from graphscope.framework.app import not_compatible_for
from graphscope.framework.app import project_to_simple
from graphscope.analytical.udf.utils import InMemoryZip
from graphscope.analytical.udf.utils import CType
__all__ = ["java_app_set"]


 #@project_to_simple
# @not_compatible_for("arrow_property", "dynamic_property")
def java_app_set(jar_path : str, java_main_class : str, vd_type, md_type):
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
    class JavaAppAssets(AppAssets):
        def __init__(self, jar_path : str , java_main_class : str, vd_type, md_type):
            vd_ctype = str(CType.from_string(vd_type)) # _t appended
            md_ctype = str(CType.from_string(md_type))
            garfile = InMemoryZip()
            tmp_jar_file = open(jar_path, 'rb')
            bytes = tmp_jar_file.read()
            garfile.append("{}".format(jar_path), bytes)
            gs_config = {
                "app": [
                    {
                        "algo": "java_app_set",
                        "context_type": "vertex_data",
                        "type": "java_pie",
                        "class_name": "gs::JavaPropertyApp",
                        "compatible_graph": ["vineyard::ArrowFragment"],
                        "vd_type": vd_ctype,
                        "md_type": md_ctype,
                        "java_main_class" : java_main_class,
                        "java_jar_path": jar_path
                    }
                ]
            }
            garfile.append(DEFAULT_GS_CONFIG_FILE, yaml.dump(gs_config))
            super().__init__("java_app_set","vertex_data",garfile.read_bytes())
        

    java_app_set_ = JavaAppAssets(jar_path=jar_path, java_main_class=java_main_class, vd_type=vd_type, md_type=md_type)
    #setattr(java_app_set_, "__decorated__", True)  # can't decorate on a decorated class
    return java_app_set_
