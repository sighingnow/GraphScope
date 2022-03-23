package com.alibaba.graphscope.graph.impl;

import com.alibaba.graphscope.graph.IdManager;
import scala.Tuple2;

public class IdManagerImpl implements IdManager {

    @Override
    public long lid2Oid(long lid) {
        return 0;
    }

    @Override
    public long gid2Oid(long lid) {
        return 0;
    }

    @Override
    public long oid2Lid(long oid) {
        return 0;
    }

    @Override
    public long oid2Gid(long oid) {
        return 0;
    }

    @Override
    public long[] getOidArray() {
        return new long[0];
    }
}
