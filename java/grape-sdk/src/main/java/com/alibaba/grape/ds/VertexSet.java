package com.alibaba.grape.ds;

import java.util.BitSet;

/**
 * Right index is exclusive
 * We don't want to introduce synchronization cost, so the underlying backend is
 * a no-thread-safe bitset.
 * <p>
 * bitSize must be specified in concurrent situations. since expansion has no concurrency control.
 */
public class VertexSet {
    private BitSet bs;
    private int left;
    // right is exclusived
    private int right;

    public VertexSet(int start, int end) {
        left = start;
        right = end;
        bs = new BitSet(right - left);
    }

    public VertexSet(long start, long end) {
        left = (int) start;
        right = (int) end;
        bs = new BitSet(right - left);
    }

    public VertexSet(VertexRange<Long> vertices) {
        left = vertices.begin().GetValue().intValue();
        right = vertices.end().GetValue().intValue();
        bs = new BitSet(right - left);
    }

    public int getLeft() {
        return left;
    }

    public int getRight() {
        return right;
    }

    public BitSet getBitSet() {
        return bs;
    }

    public boolean exist(int vid) {
        return bs.get(vid - left);
    }

    public boolean get(int vid) {
        return bs.get(vid - left);
    }

    public boolean get(Vertex<Long> vertex) {
        return bs.get(vertex.GetValue().intValue() - left);
    }

    public boolean get(long vid) {
        return bs.get((int) vid - left);
    }

    public void set(int vid) {
        bs.set(vid - left);
    }

    /**
     * This function is not thread safe, even you are assigning threads with segmented partition.
     * Because java {@code Bitset} init the {@code wordinUse = 0} and adaptively increases it, {@code ensureCapcity}
     * is to be invoked, causing problem. So we access the highest bit in initializing.
     *
     * @param vertex
     */
    public void set(Vertex<Long> vertex) {
        bs.set(vertex.GetValue().intValue() - left);
    }

    public void set(Vertex<Long> vertex, boolean newValue) {
        bs.set(vertex.GetValue().intValue() - left, newValue);
    }

    public void set(long vid) {
        bs.set((int) vid - left);
    }

    public void insert(int vid, boolean value) {
        bs.set(vid - left, value);
    }

    public void insert(Vertex<Long> vertex) {
        bs.set(vertex.GetValue().intValue() - left);
    }

    public boolean empty() {
        return bs.cardinality() <= 0;
    }

    /**
     * Check whether all [l, r) values are false;
     *
     * @param l
     * @param r
     * @return
     */
    public boolean partialEmpty(int l, int r) {
        int nextTrue = bs.nextSetBit(l);
        if (nextTrue >= r || nextTrue == -1) {
            return true;
        }
        return false;
    }

    public void clear() {
        bs.clear();
        //expand the bitset to expected range.
        bs.set(right - left, false);
    }

    /**
     * Set this vertex set with bits from another.
     *
     * @param other Another vertex set
     */
    public void assign(VertexSet other) {
        this.left = other.getLeft();
        this.right = other.getRight();
        this.bs.clear();
        this.bs = (BitSet) other.getBitSet().clone();
    }
}