/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.graphscope.example.conflict;

import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.ds.VertexRange;
import com.alibaba.grape.parallel.message.DoubleMsg;
import com.alibaba.grape.utils.FFITypeFactoryhelper;
import io.graphscope.app.PropertyDefaultAppBase;
import io.graphscope.column.DoubleColumn;
import io.graphscope.context.PropertyDefaultContextBase;
import io.graphscope.ds.PropertyAdjList;
import io.graphscope.ds.PropertyNbr;
import io.graphscope.example.sssp.PropertySSSPContext;
import io.graphscope.fragment.ArrowFragment;
import io.graphscope.parallel.PropertyMessageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConflictA implements PropertyDefaultAppBase<Long, ConflictContextA> {
    public static int flag = 1;
    public static Logger logger = LoggerFactory.getLogger(ConflictA.class.getName());

    @Override
    public void PEval(ArrowFragment<Long> fragment, PropertyDefaultContextBase<Long> context,
            PropertyMessageManager messageManager) {
        logger.info("Static flag: " + flag);
    }

    @Override
    public void IncEval(ArrowFragment<Long> fragment, PropertyDefaultContextBase<Long> context,
            PropertyMessageManager messageManager) {

    }
}
