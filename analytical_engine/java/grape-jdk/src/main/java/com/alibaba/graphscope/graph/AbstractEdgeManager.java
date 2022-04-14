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
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

/**
 * base interface for both graphx edge manager and giraph edge manager. Notice about the type
 * parameters, since giraph rely on writable. Grape_oid_t == BIZ_OID_T when graphx, neq when giraph.
 * same for BIZ_EDATA_T
 */
public abstract class AbstractEdgeManager<VID_T, GRAPE_OID_T, BIZ_OID_T, GRAPE_ED_T, BIZ_EDATA_T> {

    private static Logger logger = LoggerFactory.getLogger(AbstractEdgeManager.class.getName());

    private Vertex<VID_T> grapeVertex;
    private ArrowProjectedFragment<GRAPE_OID_T, VID_T, ?, GRAPE_ED_T> fragment;
    private PropertyNbrUnit<VID_T> nbrUnit;
    private VertexIdManager<VID_T, BIZ_OID_T> vertexIdManager;

    private long offsetBeginPtrFirstAddr, offsetEndPtrFirstAddr;
    private long nbrUnitEleSize, nbrUnitInitAddress;
    public CSRHolder csrHolder;
    protected TupleIterable edgeIterable;
    protected List<TupleIterable> edgeIterables;
    private int VID_SHIFT_BITS, VID_SIZE_IN_BYTE;
    private List<GRAPE_OID_T> oids;
    private long innerVerticesNum;
    private int vid_t, edata_t; // 0 for long ,1 for int
    private Class<? extends GRAPE_ED_T> edataClass;
    private Class<? extends BIZ_EDATA_T> bizEdataClass;

    public void init(IFragment<GRAPE_OID_T, VID_T, ?, GRAPE_ED_T> fragment,
        VertexIdManager<VID_T, BIZ_OID_T> vertexIdManager,
        Class<? extends VID_T> vidClass,
        Class<? extends GRAPE_ED_T> grapeEdataClass,
        Class<? extends BIZ_EDATA_T> bizEdataClass,
        BiConsumer<FFIByteVectorInputStream, BIZ_EDATA_T[]> consumer) {
        this.edataClass = grapeEdataClass;
        this.bizEdataClass = bizEdataClass;
        this.fragment =
            ((ArrowProjectedAdaptor<GRAPE_OID_T, VID_T, ?, GRAPE_ED_T>)
                fragment)
                .getArrowProjectedFragment();
        this.vertexIdManager = vertexIdManager;

        initFields();
        if (vidClass.equals(Long.class)) {
            VID_SHIFT_BITS = 3; // shift 3 bits <--> * 8
            VID_SIZE_IN_BYTE = 8; // long = 8bytes
            vid_t = 0;
        } else if (vidClass.equals(Integer.class)) {
            VID_SHIFT_BITS = 2;
            VID_SIZE_IN_BYTE = 4;
            vid_t = 1;
        }
        edata_t = grapeEdata2Int();
        csrHolder = new CSRHolder(this.fragment.getEdataArrayAccessor(), consumer);
        edgeIterable = new TupleIterable(csrHolder);
        edgeIterables = null;
    }

    public void init(IFragment<GRAPE_OID_T, VID_T, ?, GRAPE_ED_T> fragment,
        VertexIdManager<VID_T, BIZ_OID_T> vertexIdManager,
        Class<? extends VID_T> vidClass,
        Class<? extends GRAPE_ED_T> grapeEdataClass,
        Class<? extends BIZ_EDATA_T> bizEdataClass,
        BiConsumer<FFIByteVectorInputStream, BIZ_EDATA_T[]> consumer, int numCores) {
        init(fragment,vertexIdManager,vidClass,grapeEdataClass,bizEdataClass, consumer);
        edgeIterables = Lists.newArrayListWithCapacity(numCores);
        for (int i = 0; i < numCores; ++i){
            edgeIterables.add(new TupleIterable(csrHolder));
        }
    }

    public int getNumEdgesImpl(long lid) {
        long oeBeginOffset = JavaRuntime.getLong(offsetBeginPtrFirstAddr + lid * 8);
        long oeEndOffset = JavaRuntime.getLong(offsetEndPtrFirstAddr + lid * 8);
        return (int) (oeEndOffset - oeBeginOffset);
    }

    private void initFields() {
        nbrUnit = this.fragment.getOutEdgesPtr();
        offsetEndPtrFirstAddr = this.fragment.getOEOffsetsEndPtr();
        offsetBeginPtrFirstAddr = this.fragment.getOEOffsetsBeginPtr();
        nbrUnitEleSize = nbrUnit.elementSize();
        nbrUnitInitAddress = nbrUnit.getAddress();
        innerVerticesNum = this.fragment.getInnerVerticesNum();
        logger.info(
            "Nbrunit: [{}], offsetbeginPtr fist: [{}], end [{}]",
            nbrUnit,
            offsetBeginPtrFirstAddr,
            offsetEndPtrFirstAddr);
        logger.info(
            "nbr unit element size: {}, init address {}",
            nbrUnit.elementSize(),
            nbrUnit.getAddress());
    }


    public class TupleIterator implements Iterator<GrapeEdge<VID_T, BIZ_OID_T, BIZ_EDATA_T>> {
        private GrapeEdge<VID_T, BIZ_OID_T, BIZ_EDATA_T> grapeEdge = new GrapeEdge<>();
        private int nbrPos;
        private long numEdge;
        private BIZ_OID_T[] dstOids;
        private VID_T[] dstLids;
        private BIZ_EDATA_T[] edatas;
        private int[] nbrPositions;
        private long[] numOfEdges;
        public TupleIterator(long[] numOfEdges, int[] nbrPositions, BIZ_OID_T[] dstOids, VID_T[] dstLids, BIZ_EDATA_T[] edatas){
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
        public GrapeEdge<VID_T, BIZ_OID_T, BIZ_EDATA_T> next() {
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
        public BIZ_OID_T[] dstOids;
        public VID_T[] dstLids;
        public BIZ_EDATA_T[] edatas;
        public int[] nbrPositions;
        private BiConsumer<FFIByteVectorInputStream, BIZ_EDATA_T[]> consumer;

        public CSRHolder(TypedArray<GRAPE_ED_T> edataArray,
            BiConsumer<FFIByteVectorInputStream, BIZ_EDATA_T[]> consumer) {
            this.consumer = consumer;
            totalNumOfEdges = getTotalNumOfEdges();
            nbrUnitAddrs = new long[(int) innerVerticesNum];
            numOfEdges = new long[(int) innerVerticesNum];
            nbrPositions = new int[(int) innerVerticesNum];
            // marks the mapping between lid to start pos of nbr, i.e. offset.
            // the reason why we don't resuse oeBegin Offset is that eid may not sequential.
            edatas = (BIZ_EDATA_T[]) new Object[(int) totalNumOfEdges];
            dstOids = (BIZ_OID_T[]) new Object[(int) totalNumOfEdges];
            dstLids = (VID_T[]) new Object[(int) totalNumOfEdges];
            try {
                initArrays(edataArray);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private long getTotalNumOfEdges() {
            long largest =
                JavaRuntime.getLong(
                    offsetEndPtrFirstAddr + ((innerVerticesNum - 1) << VID_SHIFT_BITS));
            long smallest = JavaRuntime.getLong(offsetBeginPtrFirstAddr + (0 << VID_SHIFT_BITS));
            return largest - smallest;
        }

        private void initArrays(TypedArray<GRAPE_ED_T> edataArray) throws IOException {
            int tmpSum = 0;
            long oeBeginOffset, oeEndOffset;
            for (long lid = 0; lid < innerVerticesNum; ++lid) {
                long lidInAddr = (lid << VID_SHIFT_BITS);
                oeBeginOffset = JavaRuntime.getLong(offsetBeginPtrFirstAddr + lidInAddr);
                oeEndOffset = JavaRuntime.getLong(offsetEndPtrFirstAddr + lidInAddr);
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
                    if (vid_t == 0) {
                        Long dstLid = (Long) unsafe.getLong(curAddrr);
                        dstLids[index] = (VID_T) dstLid;
                        dstOids[index++] = vertexIdManager.lid2Oid(
                            (VID_T) dstLid);
                    } else {
                        Integer dstLid = (Integer) unsafe.getInt(curAddrr);
                        dstLids[index] = (VID_T) dstLid;
                        dstOids[index++] = vertexIdManager.lid2Oid(
                            (VID_T) dstLid);
                    }
                    curAddrr += nbrUnitEleSize;
                }
            }
            //fill in edata arrays.
            fillInEdataArray(edataArray);
        }

        private void fillInEdataArray(TypedArray<GRAPE_ED_T> edataArray) throws IOException {
            //first try to set directly.
            int index = 0;
            if (bizEdataClass.equals(edataClass)) {
                logger.info("biz edata {} == grape edata, try to read direct", edata_t);
                if (edata_t == 0) {
                    for (int lid = 0; lid < innerVerticesNum; ++lid) {
                        long curAddrr = nbrUnitAddrs[lid] + VID_SIZE_IN_BYTE;
                        for (int j = 0; j < numOfEdges[lid]; ++j) {
                            long eid = unsafe.getLong(curAddrr);
                            Long edata = (Long) edataArray.get(eid);
                            edatas[index++] = (BIZ_EDATA_T) edata;
                            curAddrr += nbrUnitEleSize;
                        }
                    }
                } else if (edata_t == 1) {
                    for (int lid = 0; lid < innerVerticesNum; ++lid) {
                        long curAddrr = nbrUnitAddrs[lid] + VID_SIZE_IN_BYTE;
                        for (int j = 0; j < numOfEdges[lid]; ++j) {
                            long eid = unsafe.getLong(curAddrr);
                            Integer edata = (Integer) edataArray.get(eid);
                            edatas[index++] = (BIZ_EDATA_T) edata;
                            curAddrr += nbrUnitEleSize;
                        }
                    }
                } else if (edata_t == 2) {
                    for (int lid = 0; lid < innerVerticesNum; ++lid) {
                        long curAddrr = nbrUnitAddrs[lid] + +VID_SIZE_IN_BYTE;
//                        logger.info("lid {} numEdges {}", lid, numOfEdges[lid]);
                        for (int j = 0; j < numOfEdges[lid]; ++j) {
                            long eid = unsafe.getLong(curAddrr);
                            Double edata = (Double) edataArray.get(eid);
                            edatas[index++] = (BIZ_EDATA_T) edata;
//                            logger.info("lid {} j {} eid {} edata {}", lid, j, eid, edata);
                            curAddrr += nbrUnitEleSize;
                        }
                    }
                } else if (edata_t == 3) {
                    for (int lid = 0; lid < innerVerticesNum; ++lid) {
                        long curAddrr = nbrUnitAddrs[lid] + +VID_SIZE_IN_BYTE;
                        for (int j = 0; j < numOfEdges[lid]; ++j) {
                            long eid = unsafe.getLong(curAddrr);
                            Float edata = (Float) edataArray.get(eid);
                            edatas[index++] = (BIZ_EDATA_T) edata;
                            curAddrr += nbrUnitEleSize;
                        }
                    }
                } else {
                    throw new IllegalStateException(
                        "grape edata :" + edataClass.getName() + " not recognized");
                }
            } else {
                logger.warn("Trying to load via serialization and deserialization");
                FFIByteVectorInputStream inputStream =
                    new FFIByteVectorInputStream(
                        generateEdataString(nbrUnitAddrs, numOfEdges, unsafe, edataArray));

                rebuildEdatasFromStream(inputStream, edatas);

                logger.info("Finish creating edata array");
                inputStream.getVector().delete();
            }
            if (logger.isDebugEnabled()) {
                StringBuilder sb = new StringBuilder();
                sb.append("[");
                for (int i = 0; i < edatas.length; ++i) {
                    sb.append(edatas[i] + ",");
                }
                sb.append("]");
                logger.info("edata array:" + sb.toString());
            }
        }

        //By default, we check whether BIZ_EDATA_T is the same as GRAPE_EDATA_T, if so, we just convert and set.
        //If not we turn for this function.
        public void rebuildEdatasFromStream(FFIByteVectorInputStream inputStream,
            BIZ_EDATA_T[] edatas) {
            if (consumer == null) {
                throw new IllegalStateException(
                    "You should override rebuildEdatasFromStream, since automatic conversion failed.");
            }
            consumer.accept(inputStream, edatas);

        }
    }


    public class TupleIterable implements Iterable<GrapeEdge<VID_T, BIZ_OID_T, BIZ_EDATA_T>> {

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
        public Iterator<GrapeEdge<VID_T, BIZ_OID_T, BIZ_EDATA_T>> iterator() {
            return iterator;
        }
    }

    private int grapeEdata2Int() {
        if (edataClass.equals(Long.class) || edataClass.equals(long.class)) {
            logger.info("edata: Long");
            return 0;
        } else if (edataClass.equals(Integer.class) || edataClass.equals(int.class)) {
            logger.info("edata: Int");
            return 1;
        } else if (edataClass.equals(Double.class) || edataClass.equals(double.class)) {
            logger.info("edata: Double");
            return 2;
        } else if (edataClass.equals(Float.class) || edataClass.equals(float.class)) {
            logger.info("edata: Float");
            return 3;
        } else if (edataClass.equals(String.class)) {
            logger.info("edata: String");
            return 4;
        }
        throw new IllegalStateException("Cannot recognize edata type " + edataClass);
    }

    private FFIByteVector generateEdataString(long[] nbrUnitAddrs, long[] numOfEdges, Unsafe unsafe,
        TypedArray<GRAPE_ED_T> edataArray)
        throws IOException {
        FFIByteVectorOutputStream outputStream = new FFIByteVectorOutputStream();
        switch (edata_t) {
            case 0:
                for (int lid = 0; lid < innerVerticesNum; ++lid) {
                    long curAddrr = nbrUnitAddrs[lid];
                    for (int j = 0; j < numOfEdges[lid]; ++j) {
                        long eid = unsafe.getLong(curAddrr + VID_SIZE_IN_BYTE);
                        GRAPE_ED_T edata = edataArray.get(eid);
                        Long longValue = (Long) edata;
                        outputStream.writeLong(longValue);
                        curAddrr += nbrUnitEleSize;
                    }
                }
                break;
            case 1:
                for (int lid = 0; lid < innerVerticesNum; ++lid) {
                    long curAddrr = nbrUnitAddrs[lid];
                    for (int j = 0; j < numOfEdges[lid]; ++j) {
                        long eid = unsafe.getLong(curAddrr + VID_SIZE_IN_BYTE);
                        GRAPE_ED_T edata = edataArray.get(eid);
                        Integer longValue = (Integer) edata;
                        outputStream.writeInt(longValue);
                        curAddrr += nbrUnitEleSize;
                    }
                }
                break;
            case 2:
                for (int lid = 0; lid < innerVerticesNum; ++lid) {
                    long curAddrr = nbrUnitAddrs[lid];
                    for (int j = 0; j < numOfEdges[lid]; ++j) {
                        long eid = unsafe.getLong(curAddrr + VID_SIZE_IN_BYTE);
                        GRAPE_ED_T edata = edataArray.get(eid);
                        Double longValue = (Double) edata;
                        outputStream.writeDouble(longValue);
                        curAddrr += nbrUnitEleSize;
                    }
                }
                break;
            case 3:
                for (int lid = 0; lid < innerVerticesNum; ++lid) {
                    long curAddrr = nbrUnitAddrs[lid];
                    for (int j = 0; j < numOfEdges[lid]; ++j) {
                        long eid = unsafe.getLong(curAddrr + VID_SIZE_IN_BYTE);
                        GRAPE_ED_T edata = edataArray.get(eid);
                        Float longValue = (Float) edata;
                        outputStream.writeFloat(longValue);
                        curAddrr += nbrUnitEleSize;
                    }
                }
                break;
            case 4:
                for (int lid = 0; lid < innerVerticesNum; ++lid) {
                    long curAddrr = nbrUnitAddrs[lid];
                    for (int j = 0; j < numOfEdges[lid]; ++j) {
                        long eid = unsafe.getLong(curAddrr + VID_SIZE_IN_BYTE);
                        GRAPE_ED_T edata = edataArray.get(eid);
                        String longValue = (String) edata;
                        outputStream.writeBytes(longValue);
                        curAddrr += nbrUnitEleSize;
                    }
                }
                break;
            default:
                throw new IllegalStateException("Unexpected edata type: " + edata_t);
        }
        outputStream.finishSetting();
        logger.info("Finish creating stream");
        return outputStream.getVector();
    }
}
