package com.alibaba.graphscope.graph;

import com.alibaba.fastffi.llvm4jni.runtime.JavaRuntime;
import com.alibaba.graphscope.ds.PropertyNbrUnit;
import com.alibaba.graphscope.ds.TypedArray;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

/**
 * base interface for both graphx edge manager and giraph edge manager. Notice about the type
 * parameters, since giraph rely on writable. Long == Long when graphx, neq when giraph.
 * same for Double
 */
public abstract class AbstractDoubleEdgeManager {

    private static Logger logger = LoggerFactory.getLogger(AbstractDoubleEdgeManager.class.getName());

    private Vertex<Long> grapeVertex;
    private ArrowProjectedFragment<Long, Long, ?, Double> fragment;
    private PropertyNbrUnit<Long> nbrUnit;
    private VertexIdManager<Long, Long> vertexIdManager;

    private TypedArray<Long> oeOffsetsBeginAccessor, oeOffsetsEndAccessor;
    private long nbrUnitEleSize, nbrUnitInitAddress;
    public CSRHolder csrHolder;
    protected TupleIterable edgeIterable;
    protected List<TupleIterable> edgeIterables;
    private int VID_SHIFT_BITS, VID_SIZE_IN_BYTE;
    private long innerVerticesNum;
    private Class<? extends Double> edataClass;
    private Class<? extends Double> bizEdataClass;
    private Class<? extends Long> vidClass;
    private Class<? extends Long> bizOidClass;

    public void init(IFragment<Long, Long, ?, Double> fragment,
        VertexIdManager<Long, Long> vertexIdManager,
        Class<? extends Long> bizOidClass,
        Class<? extends Long> vidClass,
        Class<? extends Double> grapeEdataClass,
        Class<? extends Double> bizEdataClass,
        BiConsumer<FFIByteVectorInputStream, Double[]> consumer) {
        this.edataClass = grapeEdataClass;
        this.bizEdataClass = bizEdataClass;
        this.bizOidClass = bizOidClass;
        this.fragment =
            ((ArrowProjectedAdaptor<Long, Long, ?, Double>)
                fragment)
                .getArrowProjectedFragment();
        this.vertexIdManager = vertexIdManager;

        initFields();
        if (vidClass.equals(Long.class)) {
            VID_SHIFT_BITS = 3; // shift 3 bits <--> * 8
            VID_SIZE_IN_BYTE = 8; // long = 8bytes
        } else if (vidClass.equals(Integer.class)) {
            VID_SHIFT_BITS = 2;
            VID_SIZE_IN_BYTE = 4;
        }
        this.vidClass = vidClass;
        csrHolder = new CSRHolder(this.fragment.getEdataArrayAccessor(), consumer);
        edgeIterable = new TupleIterable(csrHolder);
        edgeIterables = null;
    }

    public void init(IFragment<Long, Long, ?, Double> fragment,
        VertexIdManager<Long, Long> vertexIdManager,
        Class<? extends Long> bizOidClass,
        Class<? extends Long> vidClass,
        Class<? extends Double> grapeEdataClass,
        Class<? extends Double> bizEdataClass,
        BiConsumer<FFIByteVectorInputStream, Double[]> consumer, int numCores) {
        init(fragment,vertexIdManager,bizOidClass, vidClass,grapeEdataClass,bizEdataClass, consumer);
        edgeIterables = Lists.newArrayListWithCapacity(numCores);
        for (int i = 0; i < numCores; ++i){
            edgeIterables.add(new TupleIterable(csrHolder));
        }
    }

    public int getNumEdgesImpl(long lid) {
        long oeBeginOffset = oeOffsetsBeginAccessor.get(lid);
        long oeEndOffset = oeOffsetsEndAccessor.get(lid);
        return (int) (oeEndOffset - oeBeginOffset);
    }

    private void initFields() {
        nbrUnit = this.fragment.getOutEdgesPtr();
//        offsetEndPtrFirstAddr = this.fragment.getOEOffsetsEndPtr();
//        offsetBeginPtrFirstAddr = this.fragment.getOEOffsetsBeginPtr();
        oeOffsetsBeginAccessor = this.fragment.getOEOffsetsBeginAccessor();
        oeOffsetsEndAccessor = this.fragment.getOEOffsetsEndAccessor();
        nbrUnitEleSize = nbrUnit.elementSize();
        nbrUnitInitAddress = nbrUnit.getAddress();
        innerVerticesNum = this.fragment.getInnerVerticesNum();
        logger.info(
            "nbr unit element size: {}, init address {}",
            nbrUnit.elementSize(),
            nbrUnit.getAddress());
    }


    public class TupleIterator implements Iterator<GrapeEdge<Long, Long, Double>> {
        private GrapeEdge<Long, Long, Double> grapeEdge = new GrapeEdge<>();
        private int nbrPos;
        private long numEdge;
        private long[] dstOids;
        private long[] dstLids;
        private double[] edatas;
        private int[] nbrPositions;
        private long[] numOfEdges;
        public TupleIterator(long[] numOfEdges, int[] nbrPositions, long[] dstOids, long[] dstLids, double[] edatas){
            this.numOfEdges = numOfEdges;
            this.nbrPositions = nbrPositions;
            this.dstOids = dstOids;
            this.dstLids = dstLids;
            this.edatas = edatas;
        }
        public void setLid(int lid) {
            numEdge = numOfEdges[lid];
            nbrPos = nbrPositions[lid];
        }
        @Override
        public boolean hasNext() {
            return numEdge > 0;
        }

        @Override
        public GrapeEdge<Long, Long, Double> next() {
            grapeEdge.dstLid = dstLids[nbrPos];
            grapeEdge.dstOid = dstOids[nbrPos];
            grapeEdge.value = edatas[nbrPos++];
            numEdge -= 1;
            return grapeEdge;
        }
    }
    public class CSRHolder{

        private sun.misc.Unsafe unsafe = JavaRuntime.UNSAFE;
        private long totalNumOfEdges;

        public long[] nbrUnitAddrs, numOfEdges;
        public long[] dstOids;
        public long[] dstLids;
        public double[] edatas;
        public int[] nbrPositions;
        private BiConsumer<FFIByteVectorInputStream, Double[]> consumer;

        public CSRHolder(TypedArray<Double> edataArray,
            BiConsumer<FFIByteVectorInputStream, Double[]> consumer) {
            this.consumer = consumer;
            totalNumOfEdges = getTotalNumOfEdges();
            nbrUnitAddrs = new long[(int) innerVerticesNum];
            numOfEdges = new long[(int) innerVerticesNum];
            nbrPositions = new int[(int) innerVerticesNum];
            // marks the mapping between lid to start pos of nbr, i.e. offset.
            // the reason why we don't resuse oeBegin Offset is that eid may not sequential.
            edatas = new double[(int)totalNumOfEdges];
//            edatas = (Double[]) new Object[(int) totalNumOfEdges];
            dstOids = new long[(int) totalNumOfEdges];
            dstLids = new long[(int) totalNumOfEdges];
//            dstLids = (Long[]) new Object[(int) totalNumOfEdges];
            try {
                initArrays(edataArray);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private long getTotalNumOfEdges() {
            long largest = oeOffsetsEndAccessor.get(innerVerticesNum - 1);
//            long smallest = JavaRuntime.getLong(offsetBeginPtrFirstAddr + (0 << VID_SHIFT_BITS));
            long smallest = oeOffsetsBeginAccessor.get(0);
            return largest - smallest;
        }

        private void initArrays(TypedArray<Double> edataArray) throws IOException {
            int tmpSum = 0;
            long oeBeginOffset, oeEndOffset;
            for (long lid = 0; lid < innerVerticesNum; ++lid) {
//                oeBeginOffset = JavaRuntime.getLong(offsetBeginPtrFirstAddr + lidInAddr);
//                oeEndOffset = JavaRuntime.getLong(offsetEndPtrFirstAddr + lidInAddr);
                oeBeginOffset = oeOffsetsBeginAccessor.get(lid);
                oeEndOffset  = oeOffsetsEndAccessor.get(lid);
                nbrUnitAddrs[(int) lid] = nbrUnitInitAddress + (oeBeginOffset * nbrUnitEleSize);
                numOfEdges[(int) lid] = oeEndOffset - oeBeginOffset;
                tmpSum += numOfEdges[(int) lid];
            }
            if (tmpSum != totalNumOfEdges) {
                throw new IllegalStateException("not equal: " + tmpSum + ", " + totalNumOfEdges);
            }

            // deserialize back from csr.
            int index = 0;
            for (int lid = 0; lid < innerVerticesNum; ++lid) {
                long curAddrr = nbrUnitAddrs[lid];
                nbrPositions[lid] = index;
                for (int j = 0; j < numOfEdges[lid]; ++j) {
                    Long dstLid = (Long) unsafe.getLong(curAddrr);
                    dstLids[index] = (Long) dstLid;
                    dstOids[index++] = vertexIdManager.lid2Oid(
                        (Long) dstLid);
                    curAddrr += nbrUnitEleSize;
                }
            }
            //fill in edata arrays.
            fillInEdataArray(edataArray);
        }

        private void fillInEdataArray(TypedArray<Double> edataArray) throws IOException {
            //first try to set directly.
            int index = 0;
            logger.info("biz edata {} == grape edata, try to read direct");
            for (int lid = 0; lid < innerVerticesNum; ++lid) {
                long curAddrr = nbrUnitAddrs[lid] + VID_SIZE_IN_BYTE;
                for (int j = 0; j < numOfEdges[lid]; ++j) {
                    long eid = unsafe.getLong(curAddrr);
                    Double edata = (Double) edataArray.get(eid);
                    edatas[index++] = (Double) edata;
                    curAddrr += nbrUnitEleSize;
                }
            }
        }
    }


    public class TupleIterable implements Iterable<GrapeEdge<Long, Long, Double>> {

        private TupleIterator iterator;
        private CSRHolder csrHolder;

        public TupleIterable(CSRHolder csrHolder) {
            this.csrHolder = csrHolder;
            iterator = new TupleIterator(csrHolder.numOfEdges, csrHolder.nbrPositions,
                csrHolder.dstOids, csrHolder.dstLids, csrHolder.edatas);
        }

        public void setLid(long lid) {
            this.iterator.setLid((int) lid);
        }

        /**
         * Returns an iterator over elements of type {@code T}.
         *
         * @return an Iterator.
         */
        @Override
        public Iterator<GrapeEdge<Long, Long, Double>> iterator() {
            return iterator;
        }
    }
}
