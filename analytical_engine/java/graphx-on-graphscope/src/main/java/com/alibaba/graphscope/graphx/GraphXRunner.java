package com.alibaba.graphscope.graphx;

/**
 * Pregel call this method, to run pregel application with PIE worker.
 *
 * Input: GrapeGraphImpl(which contains the arrowProjectedFragment id).
 *        3 functions, initial message.
 *
 * Do :   serialize function. initial message.
 *        serialize fragment id.
 *        launch mpi processes
 */
public class GraphXRunner {

}
