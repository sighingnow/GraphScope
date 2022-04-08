package com.alibaba.graphscope.utils;

import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.ds.DestList;
import com.alibaba.graphscope.ds.ProjectedAdjList;
import com.alibaba.graphscope.ds.PropertyNbrUnit;
import com.alibaba.graphscope.ds.TypedArray;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.Loader.ArrowFragmentLoader;
import com.alibaba.graphscope.graph.GraphDataBuilder;
import com.alibaba.graphscope.graph.impl.GraphDataBuilderImpl;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Vector;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.impl.EdgePartition;
import org.apache.spark.graphx.impl.ShippableVertexPartition;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.Iterator;

/**
 * Convert a graphx graph to c++ fragment. This need copying data out of java heap, so we currently
 * only support primitives.
 *
 * @param <VD> vertex data
 * @param <ED> edge data
 */
public class GraphConverter<VD, ED> {

    private static Logger logger = LoggerFactory.getLogger(GraphConverter.class.getName());
    private static String NATIVE_UTILS = "com.alibaba.graphscope.runtime.NativeUtils";

    static {
        System.loadLibrary("grape-jni");
    }

    private Graph<VD, ED> graph;
    private Class<? extends VD> vdClass;
    private Class<? extends ED> edClass;
    private ArrowFragmentLoader fragmentLoader;
    private GraphDataBuilder<VD, ED> graphDataBuilder;

    public GraphConverter(Class<?> vdClass, Class<?> edClass) {
        //ArrowFragmentLoader loader = Factor.createLoader(protocol = "graphx)
        //also pass in the type info , vd type, ed type
        //JavaLoaderInvoker invoker = loader.getInvoker();
        //
        this.vdClass = (Class<? extends VD>) vdClass;
        this.edClass = (Class<? extends ED>) edClass;
        //check libraries loaded in this thread
//        System.loadLibrary("grape-jni");

        try {
            fragmentLoader = loadNativeUtilsClzAndCreateLoader();

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("create fragmentLoader failed");
        }
        //Set typeinfo to javaLoaderInvoker
        fragmentLoader.getJavaLoaderInvoker()
            .setTypeInfoInt(TypeUtils.generateTypeInt(Long.class, this.vdClass, this.edClass));
        graphDataBuilder = new GraphDataBuilderImpl<VD, ED>(fragmentLoader.getJavaLoaderInvoker(),
            this.vdClass, this.edClass);

    }

    public void init(Graph<VD, ED> graph) {
        this.graph = graph;
        fillVertices();
        logger.info("Finish processing vertices, now edges");
        fillEdges();
        graphDataBuilder.finishAdding();
        logger.info("Finish processing edges");
        logger.info("Totally add vertices [{}], edges [{}]", graphDataBuilder.verticesAdded(),
            graphDataBuilder.edgeAdded());
    }

    public ArrowProjectedFragment<Long, Long, VD, ED> convert() {
        //Call In memory ArrowFragmentLoader to load vertices and edges(which are already loaded offheap)
//        constructFragment(fragmentLoader.getAddress(), TypeUtils.classToInt(vdClass), TypeUtils.classToInt(edClass));
        ArrowProjectedFragment<Long, Long, VD, ED> fragment = (ArrowProjectedFragment<Long, Long, VD, ED>) invokeLoadingAndProjection(
            fragmentLoader, vdClass, edClass);
        return fragment;
    }

    private <ARRAY_TYPE> ARRAY_TYPE getFieldWithReflection(EdgePartition<ED, Object> edgePartition,
        String fieldName, Class<? extends ARRAY_TYPE> clz) {
        try {
            Field field = edgePartition.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            ARRAY_TYPE res = (ARRAY_TYPE) field.get(edgePartition);
            return res;
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static ArrowFragmentLoader createFragmentLoaderInstance(
        Class<? extends ArrowFragmentLoader> clz, long addr)
        throws InvocationTargetException, InstantiationException, IllegalAccessException {
        Constructor[] constructors = clz.getConstructors();
        for (Constructor constructor : constructors) {
            if (constructor.getParameterCount() == 1
                && constructor.getParameterTypes()[0].getName().equals("long")) {
                return clz.cast(constructor.newInstance(addr));
            }
        }
        throw new IllegalAccessException("No constructors found");
    }

    private static ArrowProjectedFragment createArrowProjectedFragmentInstance(
        Class<? extends ArrowProjectedFragment> clz, long addr)
        throws InvocationTargetException, InstantiationException, IllegalAccessException {
        Constructor[] constructors = clz.getConstructors();
        for (Constructor constructor : constructors) {
            if (constructor.getParameterCount() == 1
                && constructor.getParameterTypes()[0].getName().equals("long")) {
                return clz.cast(constructor.newInstance(addr));
            }
        }
        throw new IllegalAccessException("No constructors found");
    }

    private void fillVertices() {
        RDD<ShippableVertexPartition<VD>> rdd = graph.vertices().partitionsRDD();
        ShippableVertexPartition<VD>[] shippableVertexPartitions = (ShippableVertexPartition<VD>[]) rdd.collect();
        int totalNumVertices = 0;
        for (ShippableVertexPartition<VD> partition : shippableVertexPartitions) {
            logger.info("parition [{}] size: {}", partition, partition.size());
            totalNumVertices += partition.size();
        }
        graphDataBuilder.reserveVertex(totalNumVertices);
        for (ShippableVertexPartition<VD> partition : shippableVertexPartitions) {
            Iterator<Tuple2<Object, VD>> iterator = partition.iterator();
            try {
                while (iterator.hasNext()) {
                    Tuple2<Object, VD> tuple2 = iterator.next();
                    graphDataBuilder.addVertex((long) tuple2._1, tuple2._2, 0);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void fillEdges() {
        RDD<Tuple2<Object, EdgePartition<ED, Object>>> edges = graph.edges().partitionsRDD();
        Tuple2<Object, EdgePartition<ED, Object>>[] edgePartitions = (Tuple2<Object, EdgePartition<ED, Object>>[]) edges.collect();
        int totalEdgeNum = 0;
        for (Tuple2<Object, EdgePartition<ED, Object>> tuple : edgePartitions) {
            totalEdgeNum += tuple._2.size();
        }
        graphDataBuilder.reserveEdge(totalEdgeNum);
        for (Tuple2<Object, EdgePartition<ED, Object>> tuple : edgePartitions) {
            Integer paritionId = (Integer) tuple._1;
            EdgePartition<ED, Object> edgePartition = tuple._2;
            logger.info("edge partition ind {} size {}", paritionId, edgePartition.size()); //index
            Iterator<Edge<ED>> iterator = edgePartition.iterator();
            try {
                while (iterator.hasNext()) {
                    Edge<ED> edge = iterator.next();
                    graphDataBuilder.addEdge(edge.srcId(), edge.dstId(), edge.attr(), 0);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean check(int[] a, int[] b, long[] c) {
        if (a.length != b.length) {
            throw new IllegalStateException(
                "src id arr lenght neq to dst id arr length" + a.length + ", " + b.length);
        }
        return true;
    }

    public String arrayToString(VD[] array) {
        StringBuilder sb = new StringBuilder();
        for (VD vd : array) {
            sb.append(vd);
        }
        return sb.toString();
    }

    private static ArrowFragmentLoader loadNativeUtilsClzAndCreateLoader()
        throws ClassNotFoundException, IllegalAccessException {
        String[] libs = ClassScope.getLoadedLibraries(GraphConverter.class.getClassLoader());
        logger.info("libs: " + Arrays.toString(libs));
        Class<? extends ArrowFragmentLoader> loaderClz =
            (Class<? extends ArrowFragmentLoader>) FFITypeFactory.getType(ArrowFragmentLoader.class,
                CppClassName.ARROW_FRAGMENT_LOADER);
        logger.info("FragmentLoaderClass found {}", loaderClz.getName());
        try {
            //Thread.currentThread().setContextClassLoader(GraphConverter.class.getClassLoader());
            //MutableURLClassLoader classLoader = (MutableURLClassLoader) GraphConverter.class.getClassLoader();
            URLClassLoader classLoader = (URLClassLoader) GraphConverter.class.getClassLoader();
            logger.info("current class loader: " + classLoader);
            logger.info("fragment loader cl: " + loaderClz.getClassLoader());
            logger.info("search path 1: " + urlsToString(classLoader.getURLs()));
            logger.info("search path 2:: " + urlsToString(
                ((URLClassLoader) loaderClz.getClassLoader()).getURLs()));
            //Native functions can not be placed in this jar. must be in runtime jar.!!!!
            Class<?> nativeClz = classLoader.loadClass(NATIVE_UTILS);
            Method method = nativeClz.getDeclaredMethod("createLoader");
            if (method == null) {
                throw new IllegalStateException("No such method");
            }
            long address = (long) method.invoke(null);
            ArrowFragmentLoader fragmentLoader = createFragmentLoaderInstance(loaderClz, address);
            logger.info("created fragment loader: {}", fragmentLoader);
            return fragmentLoader;
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("fail to load native utils and create loader");
        }
    }

    private static ArrowProjectedFragment invokeLoadingAndProjection(ArrowFragmentLoader loader,
        Class<?> vdClass, Class<?> edClass) {
        Class<?> nativeClz = null;
        try {
            nativeClz = Class.forName(NATIVE_UTILS);
            Method method = nativeClz.getDeclaredMethod("invokeLoadingAndProjection", long.class,
                int.class, int.class);
            if (method == null) {
                throw new IllegalStateException("No such method");
            }
            long addr = (long) method.invoke(null, loader.getAddress(),
                TypeUtils.classToInt(vdClass),
                TypeUtils.classToInt(edClass));

            Class<? extends ArrowProjectedFragment> fragClz =
                (Class<? extends ArrowProjectedFragment>) FFITypeFactory.getType(
                    ArrowProjectedFragment.class,
                    CppClassName.ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,double>");
            if (fragClz == null) {
                throw new IllegalStateException("No projected clz found");
            }
            return createArrowProjectedFragmentInstance(fragClz, addr);
        } catch (Exception e) {
            e.printStackTrace();
        }
        throw new IllegalStateException("existing since exception ocurred");
    }

//    public static native long createArrowFragmentLoader();

//    public static native long constructFragment(long fragLoaderAddress, int vdType, int edType);

    public class ArrowProjectedEmpty implements ArrowProjectedFragment {

        @Override
        public ProjectedAdjList getIncomingAdjList(Vertex vertex) {
            return null;
        }

        @Override
        public ProjectedAdjList getOutgoingAdjList(Vertex vertex) {
            return null;
        }

        @Override
        public PropertyNbrUnit getOutEdgesPtr() {
            return null;
        }

        @Override
        public long getOEOffsetsBeginPtr() {
            return 0;
        }

        @Override
        public long getOEOffsetsEndPtr() {
            return 0;
        }

        @Override
        public TypedArray getEdataArrayAccessor() {
            return null;
        }

        /**
         * Get the number of inner vertices.
         *
         * @return number of inner vertices.
         */
        @Override
        public long getInnerVerticesNum() {
            return 0;
        }

        /**
         * Get the number of outer vertices.
         *
         * @return umber of outer vertices.
         */
        @Override
        public long getOuterVerticesNum() {
            return 0;
        }

        /**
         * Obtain vertex range contains all inner vertices.
         *
         * @return vertex range.
         */
        @Override
        public VertexRange innerVertices() {
            return null;
        }

        /**
         * Obtain vertex range contains all outer vertices.
         *
         * @return vertex range.
         */
        @Override
        public VertexRange outerVertices() {
            return null;
        }

        /**
         * Check whether a vertex is a inner vertex for a fragment.
         *
         * @param vertex querying vertex.
         * @return true if is inner vertex.
         */
        @Override
        public boolean isInnerVertex(Vertex vertex) {
            return false;
        }

        /**
         * Check whether a vertex is a outer vertex for a fragment.
         *
         * @param vertex querying vertex.
         * @return true if is outer vertex.
         */
        @Override
        public boolean isOuterVertex(Vertex vertex) {
            return false;
        }

        /**
         * Check whether a vertex, represented in OID_T, is a inner vertex. If yes, if true and put
         * inner representation id in the second param. Else return false.
         *
         * @param oid    querying vertex in OID_T.
         * @param vertex placeholder for VID_T, if oid belongs to this fragment.
         * @return inner vertex or not.
         */
        @Override
        public boolean getInnerVertex(Object oid, Vertex vertex) {
            return false;
        }

        /**
         * Check whether a vertex, represented in OID_T, is a outer vertex. If yes, if true and put
         * outer representation id in the second param. Else return false.
         *
         * @param oid    querying vertex in OID_T.
         * @param vertex placeholder for VID_T, if oid doesn't belong to this fragment.
         * @return outer vertex or not.
         */
        @Override
        public boolean getOuterVertex(Object oid, Vertex vertex) {
            return false;
        }

        /**
         * Obtain vertex id from original id, only for inner vertex.
         *
         * @param vertex querying vertex.
         * @return original id.
         */
        @Override
        public Object getInnerVertexId(Vertex vertex) {
            return null;
        }

        /**
         * Obtain vertex id from original id, only for outer vertex.
         *
         * @param vertex querying vertex.
         * @return original id.
         */
        @Override
        public Object getOuterVertexId(Vertex vertex) {
            return null;
        }

        /**
         * Convert from global id to an inner vertex handle.
         *
         * @param gid    Input global id.
         * @param vertex Output vertex handle.
         * @return True if exists an inner vertex of this fragment with global id as gid, false
         * otherwise.
         */
        @Override
        public boolean innerVertexGid2Vertex(Object gid, Vertex vertex) {
            return false;
        }

        /**
         * Convert from global id to an outer vertex handle.
         *
         * @param gid    Input global id.
         * @param vertex Output vertex handle.
         * @return True if exists an outer vertex of this fragment with global id as gid, false
         * otherwise.
         */
        @Override
        public boolean outerVertexGid2Vertex(Object gid, Vertex vertex) {
            return false;
        }

        /**
         * Convert from inner vertex handle to its global id.
         *
         * @param vertex Input vertex handle.
         * @return Global id of the vertex.
         */
        @Override
        public Object getOuterVertexGid(Vertex vertex) {
            return null;
        }

        /**
         * Convert from outer vertex handle to its global id.
         *
         * @param vertex Input vertex handle.
         * @return Global id of the vertex.
         */
        @Override
        public Object getInnerVertexGid(Vertex vertex) {
            return null;
        }

        /**
         * Return the incoming edge destination fragment ID list of a inner vertex.
         *
         * <p>For inner vertex v of fragment-0, if outer vertex u and w are parents of v. u belongs
         * to fragment-1 and w belongs to fragment-2, then 1 and 2 are in incoming edge destination
         * fragment ID list of v.
         *
         * <p>This method is encapsulated in the corresponding sending message API,
         * SendMsgThroughIEdges, so it is not recommended to use this method directly in application
         * programs.
         *
         * @param vertex Input vertex.
         * @return The incoming edge destination fragment ID list.
         */
        @Override
        public DestList inEdgeDests(Vertex vertex) {
            return null;
        }

        /**
         * Return the outgoing edge destination fragment ID list of a inner vertex.
         *
         * <p>For inner vertex v of fragment-0, if outer vertex u and w are children of v. u
         * belongs to fragment-1 and w belongs to fragment-2, then 1 and 2 are in outgoing edge
         * destination fragment ID list of v.
         *
         * <p>This method is encapsulated in the corresponding sending message API,
         * SendMsgThroughOEdges, so it is not recommended to use this method directly in application
         * programs.
         *
         * @param vertex Input vertex.
         * @return The outgoing edge destination fragment ID list.
         */
        @Override
        public DestList outEdgeDests(Vertex vertex) {
            return null;
        }

        /**
         * Get both the in edges and out edges.
         *
         * @param vertex query vertex.
         * @return The outgoing and incoming edge destination fragment ID list.
         */
        @Override
        public DestList inOutEdgeDests(Vertex vertex) {
            return null;
        }

        @Override
        public Object getData(Vertex vertex) {
            return null;
        }

        /**
         * @return The id of current fragment.
         */
        @Override
        public int fid() {
            return 0;
        }

        /**
         * Number of fragments.
         *
         * @return number of fragments.
         */
        @Override
        public int fnum() {
            return 0;
        }

        /**
         * Returns the number of edges in this fragment.
         *
         * @return the number of edges in this fragment.
         */
        @Override
        public long getEdgeNum() {
            return 0;
        }

        /**
         * Returns the number of vertices in this fragment.
         *
         * @return the number of vertices in this fragment.
         */
        @Override
        public Object getVerticesNum() {
            return null;
        }

        /**
         * Returns the number of vertices in the entire graph.
         *
         * @return The number of vertices in the entire graph.
         */
        @Override
        public long getTotalVerticesNum() {
            return 0;
        }

        /**
         * Get all vertices referenced to this fragment.
         *
         * @return A vertex set can be iterate on.
         */
        @Override
        public VertexRange vertices() {
            return null;
        }

        /**
         * Get the vertex handle from the original id.
         *
         * @param oid    input original id.
         * @param vertex output vertex handle
         * @return If find the vertex in this fragment, return true. Otherwise, return false.
         */
        @Override
        public boolean getVertex(Object oid, Vertex vertex) {
            return false;
        }

        /**
         * Get the original Id of a vertex.
         *
         * @param vertex querying vertex.
         * @return original id.
         */
        @Override
        public Object getId(Vertex vertex) {
            return null;
        }

        /**
         * To which fragment the vertex belongs.
         *
         * @param vertex querying vertex.
         * @return frag id.
         */
        @Override
        public int getFragId(Vertex vertex) {
            return 0;
        }

        @Override
        public int getLocalInDegree(Vertex vertex) {
            return 0;
        }

        @Override
        public int getLocalOutDegree(Vertex vertex) {
            return 0;
        }

        @Override
        public boolean gid2Vertex(Object gid, Vertex vertex) {
            return false;
        }

        @Override
        public Object vertex2Gid(Vertex vertex) {
            return null;
        }

        @Override
        public long getAddress() {
            return 0;
        }
    }

    public static class ClassScope {

        private static java.lang.reflect.Field LIBRARIES = null;

        static {
            try {
                LIBRARIES = ClassLoader.class.getDeclaredField("loadedLibraryNames");
            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            }
            if (LIBRARIES != null) {
                LIBRARIES.setAccessible(true);
            } else {
                logger.error("fail to get field");
            }
        }

        public static String[] getLoadedLibraries(final ClassLoader loader)
            throws IllegalAccessException {
            final Vector<String> libraries = (Vector<String>) LIBRARIES.get(loader);
            return libraries.toArray(new String[]{});
        }
    }

    private static String urlsToString(URL[] urls) {
        StringBuilder sb = new StringBuilder();
        for (URL url : urls) {
            sb.append(url);
            sb.append(",");
        }
        return sb.toString();
    }

}
