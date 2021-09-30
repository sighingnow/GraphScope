package com.alibaba.grape.fragment;

import com.alibaba.ffi.FFIVector;
import com.alibaba.grape.graph.loader.EVFileLoaderTest;
import org.junit.Before;
import org.junit.Test;


public class ImmutableEdgecutFragmentTest {

    private EVFileLoaderTest evFileLoaderTest;
    public FFIVector<FFIVector<Long>> vidBuffers;
    public FFIVector<FFIVector<Long>> vdataBuffers;
    public FFIVector<FFIVector<Long>> esrcBuffers;
    public FFIVector<FFIVector<Long>> edstBuffers;
    public FFIVector<FFIVector<Double>> edataBuffers;

    @Before
    public void prepare() {
        evFileLoaderTest = new EVFileLoaderTest();
        evFileLoaderTest.init();
        evFileLoaderTest.test1();
        vidBuffers = evFileLoaderTest.vidBuffers;
        vdataBuffers = evFileLoaderTest.vdataBuffers;
        esrcBuffers = evFileLoaderTest.esrcBuffers;
        edstBuffers = evFileLoaderTest.edstBuffers;
        edataBuffers = evFileLoaderTest.edataBuffers;
    }

    @Test
    public void loadInCpp() {
        //FIXME: To load in cpp requires mpi_comm, how to get rid of this?
    }
}
