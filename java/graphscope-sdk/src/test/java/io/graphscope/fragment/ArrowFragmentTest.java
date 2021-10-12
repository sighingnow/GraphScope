package io.graphscope.fragment;

import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.ds.VertexRange;
import com.alibaba.grape.utils.FFITypeFactoryhelper;
import io.graphscope.ds.*;
import io.graphscope.utils.VineyardHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static io.graphscope.utils.CppClassName.ARROW_FRAGMENT;
import static io.graphscope.utils.JNILibraryName.VINEYARD_JNI_LIBRARY;

public class ArrowFragmentTest {
    static {
        try {
            System.loadLibrary(VINEYARD_JNI_LIBRARY);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static final String arrowFragmentFFIName = ARROW_FRAGMENT + "<int64_t>";
    //    private static final String socket = "/tmp/vineyard.sock.frag";
    public static List<String> params;
    public long fragObjectId;
    private ArrowFragment<Long> fragment;
    private static Process vineyard_server;

//    @BeforeClass
//    public static void startVineyardServer() {
//        loadConf();
//        //commands has to be concat together
//        String[] commands = {"/bin/bash", "-c", "vineyardd -socket=" + socket};
//        ProcessBuilder builder = new ProcessBuilder();
//        try {
//            builder.command(commands);
//            File log = new File("vineyardd-log");
//            File error = new File("vineyardd-error");
//            builder.redirectOutput(ProcessBuilder.Redirect.appendTo(error));
//            builder.redirectError(ProcessBuilder.Redirect.appendTo(log));
//            vineyard_server = builder.start();
//            Thread.sleep(4000);
//            System.out.println("vineyardd server started, listening to" + socket);
//        } catch (IOException | InterruptedException e) {
//            e.printStackTrace();
//            Assert.fail("exeception");
//        } finally {
//            System.out.println("finally");
//        }
//    }


    public void cppLoadFragment() {
        loadConf();
        //String concat = "/home/admin/v6d/build/bin/arrow_fragment_test /tmp/vineyard.sock 2 \"/home/admin/v6d/gstest/modern_graph/knows.csv#header_row=true&delimiter=|&label=knows&src_label=person&dst_label=person\" \"/home/admin/v6d/gstest/modern_graph/created.csv#header_row=true&delimiter=|&label=created&src_label=person&dst_label=software\" 2 \"/home/admin/v6d/gstest/modern_graph/person.csv#label=person&header_row=true&delimiter=|\" \"/home/admin/v6d/gstest/modern_graph/software.csv#label=software&header_row=true&delimiter=|\"";
        String concat = String.join(" ", params);
        System.out.println(concat);
        String[] commands = {"/bin/bash", "-c", concat};
        ProcessBuilder builder = new ProcessBuilder();
        try {
            builder.command(commands);
            File log = new File("vineyard-exec-log");
            File error = new File("vineyard-exec-error");
            builder.redirectOutput(ProcessBuilder.Redirect.appendTo(error));
            builder.redirectError(ProcessBuilder.Redirect.appendTo(log));
            Process process = builder.start();

            int exitVal = process.waitFor();
            if (exitVal == 0) {
                System.out.println("Success!");
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("vineyard-exec-log")))) {
                    String tmp;
                    while ((tmp = reader.readLine()) != null) {
                        if (tmp.indexOf("[frag-0]") != -1) {
                            fragObjectId = Long.parseUnsignedLong(tmp.substring(tmp.lastIndexOf(":") + 1).trim(), 10);
                        }
                    }
                }
                System.out.println("frag objectid " + fragObjectId);
            } else {
                Assert.fail("exit with" + exitVal);
                return;
            }
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("IO exeception");
        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.fail("interrupted");
        } finally {
            System.out.println("finally");
        }
    }

    @Test
    public void testFFIFragment() {
        cppLoadFragment();
        //create fragment
        try {
            Class<ArrowFragment> fragmentClass = (Class<ArrowFragment>) FFITypeFactory.getType(arrowFragmentFFIName);
            Constructor[] constructors = fragmentClass.getConstructors();
            if (constructors.length == 0) {
                Assert.fail("no constructors found for " + arrowFragmentFFIName);
            }
            for (Constructor constructor : constructors) {
                System.out.println(constructor);
                if (constructor.getParameterCount() == 1 && constructor.getParameterTypes()[0].getName().equals("long")) {
                    System.out.println("frag object id " + fragObjectId);
                    long fragAddr = VineyardHelper.getAddressFromObjectID(fragObjectId);
                    System.out.println(" fragAddr " + fragAddr);
                    fragment = fragmentClass.cast(constructor.newInstance(fragAddr));
                    System.out.println(fragment);
                }
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            Assert.fail("didn't find class " + arrowFragmentFFIName);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("failed for other exceptions");
        }

        //test some simple getter
        System.out.println("fragment address " + fragment.getAddress());
        System.out.println("edge label num" + fragment.edgeLabelNum());
        System.out.println("vertex label num" + fragment.vertexLabelNum());
        System.out.println("fid " + fragment.fid());
        System.out.println("fnum " + fragment.fnum());


        for (int i = 0; i < fragment.edgeLabelNum(); ++i) {
            System.out.print("edge property num for edge label [" + i + "] " + fragment.edgePropertyNum(i) + ", ");
        }
        System.out.println();
        for (int i = 0; i < fragment.vertexLabelNum(); ++i) {
            System.out.print("vertex property num for vertex label [" + i + "] " + fragment.vertexPropertyNum(i) + ", ");
        }
        System.out.println();

        VertexRange<Long> innerVertices = fragment.innerVertices(0);
        VertexRange<Long> outerVertices = fragment.outerVertices(0);
        VertexRange<Long> fragVertices = fragment.vertices(0);
        System.out.println("inner vertices(" + innerVertices.begin().GetValue() + ", " + innerVertices.end().GetValue() + ")");
        System.out.println("outer vertices(" + outerVertices.begin().GetValue() + ", " + outerVertices.end().GetValue() + ")");
        System.out.println("frag vertices(" + fragVertices.begin().GetValue() + ", " + fragVertices.end().GetValue() + ")");
        Assert.assertTrue(fragment.isInnerVertex(innerVertices.begin()));
        Assert.assertFalse(fragment.isInnerVertex(outerVertices.begin()));
        Assert.assertFalse(fragment.isOuterVertex(innerVertices.begin()));

        long oid = fragment.getOid(fragment.innerVertices(0).begin());
        System.out.println("oid for vertex<" + fragment.innerVertices(0) + "> " + oid);
        oid = 1L;
        Vertex<Long> cur = FFITypeFactoryhelper.newVertexLong();
        fragment.getVertex(0, oid, cur);
        System.out.println("lid for oid <" + oid + "> " + cur.GetValue());

        PropertyAdjList<Long> outAdjList = fragment.getOutgoingAdjList(cur, 0);
        System.out.println("outgoing adjlist for vertex " + cur.GetValue() + " oid " + fragment.getOid(cur) +
                " [" + outAdjList.begin().neighbor().GetValue() + " , " + outAdjList.end().neighbor().GetValue() + "] " + outAdjList.size());

        PropertyAdjList<Long> inAdjList = fragment.getIncomingAdjList(cur, 0);
        System.out.println("incoming adjlist for vertex " + cur.GetValue() + " oid " + fragment.getOid(cur) +
                " [" + inAdjList.begin().neighbor().GetValue() + " , " + inAdjList.end().neighbor().GetValue() + "]" + inAdjList.size());

        for (PropertyNbr<Long> nbr : outAdjList.iterator()) {
            System.out.println(nbr.neighbor().GetValue() + ": " + nbr.getDouble(0));
        }
        for (PropertyNbr<Long> nbr : inAdjList.iterator()) {
            System.out.println(nbr.neighbor().GetValue() + ": " + nbr.getDouble(0));
        }
        EdgeDataColumn<Double> columnDouble = fragment.edgeDataColumn(0, 0, 2.0);
        PropertyRawAdjList<Long> outRawAdjList = fragment.getOutgoingRawAdjList(cur, 0);
        System.out.println("outgoing raw adjlist for vertex " + cur.GetValue() + " oid " + fragment.getOid(cur) +
                " [" + outAdjList.begin().neighbor().GetValue() + " , " + outAdjList.end().neighbor().GetValue() + "] " + outAdjList.size());
        for (PropertyNbrUnit<Long> nbr : outRawAdjList.iterator()) {
            System.out.println("edge data " + nbr.getNeighbor().GetValue() + "/ " + nbr.vid() + ": " + columnDouble.get(nbr));
        }
        // the first property is string ,second is uint64_t
        VertexDataColumn<Long> vertexDataColumn = fragment.vertexDataColumn(0, 1, 2L);
        for (PropertyNbrUnit<Long> nbr : outRawAdjList.iterator()) {
            System.out.println("vertex data " + nbr.getNeighbor().GetValue() + "/ " + nbr.vid() + ": " + vertexDataColumn.get(nbr.getNeighbor()));
        }

        System.out.println("Get vertex data, vertex<" + cur.GetValue() + "> " + fragment.getLongData(cur, 1));
        System.out.println("Vertex has child: " + fragment.hasChild(cur, 0));
        System.out.println("vertex has parent " + fragment.hasParent(cur, 0));
        System.out.println("vertex out degree " + fragment.getLocalOutDegree(cur, 0) + " in degree " + fragment.getLocalInDegree(cur, 0));
        for (Vertex<Long> v : innerVertices.locals()) {
            System.out.println("Get vertex data, vertex<" + v.GetValue() + "> " + fragment.getLongData(v, 1));
        }
        long gid = fragment.vertex2Gid(cur);
        {
            Vertex<Long> tmp = FFITypeFactoryhelper.newVertexLong();
            Assert.assertTrue(fragment.gid2Vertex(gid, tmp));
            Assert.assertTrue(cur.GetValue().intValue() == tmp.GetValue().longValue());
        }

        {
            Vertex<Long> tmp = FFITypeFactoryhelper.newVertexLong();
            Assert.assertTrue(fragment.getInnerVertex(0, 1L, tmp));
            Assert.assertTrue(tmp.GetValue().longValue() == cur.GetValue().longValue());
            Assert.assertFalse(fragment.getOuterVertex(0, 1L, tmp));
        }

        {
            Long curOid = fragment.getInnerVertexOid(cur);
            Assert.assertTrue(curOid.longValue() == oid);
        }

        {
            Vertex<Long> vertexGid = FFITypeFactoryhelper.newVertexLong();
            Vertex<Long> vertexLid = FFITypeFactoryhelper.newVertexLong();
            Assert.assertTrue(fragment.oid2Gid(0, oid, vertexGid));
            Assert.assertTrue(fragment.gid2Oid(vertexGid.GetValue().longValue()) == oid);
            Assert.assertTrue(fragment.innerVertexGid2Vertex(vertexGid.GetValue(), vertexLid));
            System.out.println("vertex lid for gid " + vertexGid.GetValue() + " is " + vertexLid.GetValue());
            Assert.assertTrue(fragment.getOid(vertexLid).longValue() == oid);
        }
        {
            System.out.println("directed: " + fragment.directed());
        }
    }

    public static void loadConf() {
        params = new ArrayList<>();
        try {
            Properties properties = new Properties();
            InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("vineyard.properties");
            properties.load(in);
            params.add(properties.getProperty("vineyard_cmd"));
            params.add(properties.getProperty("socket_name"));
//            params.add(socket);
            params.add(properties.getProperty("elabel_num"));
            params.addAll(Arrays.asList(properties.getProperty("efiles").split(":")));
            params.add(properties.getProperty("vlabel_num"));
            params.addAll(Arrays.asList(properties.getProperty("vfiles").split(":")));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
            return;
        }
    }

//    @AfterClass
//    public static void shutdown() throws InterruptedException {
//        vineyard_server.destroyForcibly();
//        vineyard_server.waitFor();
//        while (vineyard_server.isAlive()) {
//            Thread.sleep(500);
//            System.out.println("waiting for to be killed");
//        }
//        System.out.println("shutdown vineyard server");
//    }

}

