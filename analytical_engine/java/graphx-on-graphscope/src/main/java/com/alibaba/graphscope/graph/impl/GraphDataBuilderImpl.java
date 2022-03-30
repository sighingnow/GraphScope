package com.alibaba.graphscope.graph.impl;

import com.alibaba.graphscope.fragment.Loader.JavaLoaderInvoker;
import com.alibaba.graphscope.graph.GraphDataBuilder;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVecVector;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.stdcxx.FFIIntVecVector;
import com.alibaba.graphscope.stdcxx.FFIIntVector;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphDataBuilderImpl<VD, ED> implements GraphDataBuilder<VD, ED> {

    private static Logger logger = LoggerFactory.getLogger(GraphDataBuilderImpl.class.getName());

    private int threadNum;
    private int workerId;
    private int workerNum;

    private Class<? extends VD> vdClass;
    private Class<? extends ED> edClass;

    private AtomicLong vertexCounter, edgeCounter;
    private FFIByteVecVector oids, vdatas, esrcs, edsts, edatas;

    private FFIIntVecVector oidOffsets, vdataOffsets, esrcOffsets, edstOffsets, edataOffsets;

    private FFIByteVectorOutputStream[] oidsOutputStream, vdatasOutputStream, edgeSrcIdOutputStream, edgeDstOutputStream, edgeDataOutStream;

    private FFIIntVector[] idOffsetsArr, vdataOffsetsArr, edgeSrcIdOffsetArr, edgeDstIdOffsetArr, edgeDataOffsetsArr;

    public GraphDataBuilderImpl(JavaLoaderInvoker javaLoaderInvoker, Class<? extends VD> vdClass, Class<? extends ED> edClass) {
        this.workerNum = javaLoaderInvoker.workerNum();
        this.workerId = javaLoaderInvoker.workerId();
        this.threadNum = javaLoaderInvoker.loadingThreadNum();
        this.vdClass = vdClass;
        this.edClass = edClass;
        if (threadNum <= 0 || workerId < 0 || workerNum <= 0) {
            throw new IllegalStateException(
                "thread num " + threadNum + " worker id " + workerId + " workerNum " + workerNum);
        }
        vertexCounter = new AtomicLong(0);
        edgeCounter = new AtomicLong(0);
        initVector(javaLoaderInvoker);
        checkIntegrity();
        initStream();
    }

    @Override
    public void reserveVertex(int numVertices) {
        int leastSize = numVertices * 8;
        for (int i = 0; i < threadNum; ++i) {
            oidsOutputStream[i].getVector().resize(leastSize);
            oidsOutputStream[i].getVector().touch();
            vdatasOutputStream[i].getVector().resize(leastSize);
            vdatasOutputStream[i].getVector().touch();

            idOffsetsArr[i].reserve(numVertices); // may not enough
            idOffsetsArr[i].touch();
            vdataOffsetsArr[i].reserve(numVertices);
            vdataOffsetsArr[i].touch();
        }
    }

    @Override
    public void reserveEdge(int numEdges){
        //FIXME: currently only for primitives.
        int leastSize = numEdges * 8;
        for (int i = 0; i < threadNum; ++i) {
            edgeSrcIdOutputStream[i].getVector().resize(leastSize);
            edgeSrcIdOutputStream[i].getVector().touch();
            edgeDstOutputStream[i].getVector().resize(leastSize);
            edgeDstOutputStream[i].getVector().touch();
            edgeDataOutStream[i].getVector().resize(leastSize);
            edgeDataOutStream[i].getVector().touch();

            edgeSrcIdOffsetArr[i].reserve(numEdges); // may not enough
            edgeSrcIdOffsetArr[i].touch();
            edgeDstIdOffsetArr[i].reserve(numEdges);
            edgeDstIdOffsetArr[i].touch();
            edgeDataOffsetsArr[i].reserve(numEdges);
            edgeDataOffsetsArr[i].touch();
        }
    }

    @Override
    public void addEdge(long srcOid, long dstOid, ED edata, int threadId) throws IOException {
        logger.info("Adding Edge [{}] -> [{}] : [{}]", srcOid,dstOid, edata);
        int bytesEdgeSrcOffset = 0, bytesEdgeDstOffset = 0, bytesDataOffsets = 0;

        bytesEdgeSrcOffset = (int) -edgeSrcIdOutputStream[threadId].bytesWriten();
        edgeSrcIdOutputStream[threadId].writeLong(srcOid);
        bytesEdgeSrcOffset += edgeSrcIdOutputStream[threadId].bytesWriten();
        edgeSrcIdOffsetArr[threadId].push_back(bytesEdgeSrcOffset);

        bytesEdgeDstOffset = (int) -edgeDstOutputStream[threadId].bytesWriten();
        edgeDstOutputStream[threadId].writeLong(dstOid);
        bytesEdgeDstOffset += edgeDstOutputStream[threadId].bytesWriten();
        edgeDstIdOffsetArr[threadId].push_back(bytesEdgeDstOffset);

        bytesDataOffsets = (int) -edgeDataOutStream[threadId].bytesWriten();
        if (edClass.equals(double.class) || edClass.equals(Double.class)){
            edgeDataOutStream[threadId].writeDouble((Double) edata);
        }
        else if (edClass.equals(long.class) || edClass.equals(Long.class)){
            edgeDataOutStream[threadId].writeLong((Long) edata);
        }
        else if (edClass.equals(int.class) || edClass.equals(Integer.class)){
            edgeDataOutStream[threadId].writeInt((Integer) edata);
        }
        else {
            throw new IllegalStateException("Edclass not supported " + edClass.getName());
        }
        bytesDataOffsets += edgeDataOutStream[threadId].bytesWriten();
        edgeDataOffsetsArr[threadId].push_back(bytesDataOffsets);

        edgeCounter.getAndAdd(1);
    }

    @Override
    public void addVertex(long oid, VD vdata, int threadId) throws IOException {
        logger.info("Adding Vertex [{}] : [{}]", oid,vdata);
        int bytes = (int) -oidsOutputStream[threadId].bytesWriten();
        oidsOutputStream[threadId].writeLong(oid);
        bytes += oidsOutputStream[threadId].bytesWriten();
        idOffsetsArr[threadId].push_back(bytes);

        int bytes2 = (int) -vdatasOutputStream[threadId].bytesWriten();
        if (vdClass.equals(double.class) || vdClass.equals(Double.class)){
            edgeDataOutStream[threadId].writeDouble((Double) vdata);
        }
        else if (vdClass.equals(long.class) || vdClass.equals(Long.class)){
            edgeDataOutStream[threadId].writeLong((Long) vdata);
        }
        else if (vdClass.equals(int.class) || vdClass.equals(Integer.class)){
            edgeDataOutStream[threadId].writeInt((Integer) vdata);
        }
        else {
            throw new IllegalStateException("Vdclass not supported " + vdClass.getName());
        }
        bytes2 += vdatasOutputStream[threadId].bytesWriten();
        vdataOffsetsArr[threadId].push_back(bytes2);

        vertexCounter.getAndAdd(1);
    }

    @Override
    public void finishAdding() {
        for (int i = 0; i < threadNum; ++i) {
            oidsOutputStream[i].finishSetting();
            vdatasOutputStream[i].finishSetting();
            edgeSrcIdOutputStream[i].finishSetting();
            edgeDstOutputStream[i].finishSetting();
            edgeDataOutStream[i].finishSetting();

            idOffsetsArr[i].finishSetting();
            vdataOffsetsArr[i].finishSetting();
            edgeSrcIdOffsetArr[i].finishSetting();
            edgeDstIdOffsetArr[i].finishSetting();
            edgeDataOffsetsArr[i].finishSetting();
        }
    }

    @Override
    public long verticesAdded() {
        return vertexCounter.get();
    }

    @Override
    public long edgeAdded() {
        return edgeCounter.get();
    }

    private void initVector(JavaLoaderInvoker javaLoaderInvoker) {
        this.oids = new FFIByteVecVector(javaLoaderInvoker.getOids().getAddress());
        this.vdatas = new FFIByteVecVector(javaLoaderInvoker.getVdatas().getAddress());
        this.esrcs = new FFIByteVecVector(javaLoaderInvoker.getEdgeSrcs().getAddress());
        this.edsts = new FFIByteVecVector(javaLoaderInvoker.getEdgeDsts().getAddress());
        this.edatas = new FFIByteVecVector(javaLoaderInvoker.getEdgeDatas().getAddress());

        this.oidOffsets = new FFIIntVecVector(javaLoaderInvoker.getOidOffsets().getAddress());
        this.vdataOffsets = new FFIIntVecVector(javaLoaderInvoker.getVdataOffsets().getAddress());
        this.esrcOffsets = new FFIIntVecVector(javaLoaderInvoker.getEdgeSrcOffsets().getAddress());
        this.edstOffsets = new FFIIntVecVector(javaLoaderInvoker.getEdgeDstOffsets().getAddress());
        this.edataOffsets = new FFIIntVecVector(javaLoaderInvoker.getEdgeDataOffsets().getAddress());
    }

    private void initStream() {
        oidsOutputStream = new FFIByteVectorOutputStream[threadNum];
        vdatasOutputStream = new FFIByteVectorOutputStream[threadNum];
        edgeSrcIdOutputStream = new FFIByteVectorOutputStream[threadNum];
        edgeDstOutputStream = new FFIByteVectorOutputStream[threadNum];
        edgeDataOutStream = new FFIByteVectorOutputStream[threadNum];

        for (int i = 0; i < threadNum; ++i) {
            oidsOutputStream[i] = new FFIByteVectorOutputStream((FFIByteVector) oids.get(i));
            vdatasOutputStream[i] =
                new FFIByteVectorOutputStream((FFIByteVector) vdatas.get(i));
            edgeSrcIdOutputStream[i] =
                new FFIByteVectorOutputStream((FFIByteVector) esrcs.get(i));
            edgeDstOutputStream[i] =
                new FFIByteVectorOutputStream((FFIByteVector) edsts.get(i));
            edgeDataOutStream[i] =
                new FFIByteVectorOutputStream((FFIByteVector) edatas.get(i));
        }

        this.idOffsetsArr = new FFIIntVector[threadNum];
        this.vdataOffsetsArr = new FFIIntVector[threadNum];
        this.edgeSrcIdOffsetArr = new FFIIntVector[threadNum];
        this.edgeDstIdOffsetArr = new FFIIntVector[threadNum];
        this.edgeDataOffsetsArr = new FFIIntVector[threadNum];

        for (int i = 0; i < threadNum; ++i) {
            idOffsetsArr[i] = (FFIIntVector) oidOffsets.get(i);
            vdataOffsetsArr[i] = (FFIIntVector) vdataOffsets.get(i);
            edgeSrcIdOffsetArr[i] = (FFIIntVector) esrcOffsets.get(i);
            edgeDstIdOffsetArr[i] = (FFIIntVector) edstOffsets.get(i);
            edgeDataOffsetsArr[i] = (FFIIntVector) edataOffsets.get(i);
        }
    }

    private void checkIntegrity() {
        if ((oids.getAddress() == 0 || vdatas.getAddress() == 0 || esrcs.getAddress() == 0
            || edsts.getAddress() == 0)
            || ((oidOffsets.getAddress() == 0) || vdataOffsets.getAddress() == 0
            || esrcOffsets.getAddress() == 0 || edstOffsets.getAddress() == 0)) {
            throw new IllegalStateException("ffivectors empty");
        }
    }
}
