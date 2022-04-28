package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.ds.Vertex;

public class VertexImpl<VID_T> implements Vertex<VID_T> {
    private VID_T vid;
    public VertexImpl(VID_T value){
        this.vid = value;
    }
    /**
     * Return a <em>deep</em> copy of current vertex.
     *
     * @return the copied vertex.
     */
    @Override
    public Vertex<VID_T> copy() {
        return new VertexImpl<VID_T>(vid);
    }

    /**
     * Note this is not necessary to be a prefix increment
     *
     * @return current vertex with vertex.id + 1
     */
    @Override
    public Vertex<VID_T> inc() {
        return this;
    }

    /**
     * Judge whether Two vertex id are the same.
     *
     * @param vertex vertex to compare with.
     * @return equal or not.
     */
    @Override
    public boolean eq(Vertex<VID_T> vertex) {
        return false;
    }

    /**
     * Get vertex id.
     *
     * @return vertex id.
     */
    @Override
    public VID_T GetValue() {
        return vid;
    }

    /**
     * Set vertex id.
     *
     * @param id id to be set.
     */
    @Override
    public void SetValue(VID_T id) {
        this.vid = id;
    }

    @Override
    public void delete() {

    }

    @Override
    public long getAddress() {
        return 0;
    }
}
