package com.alibaba.graphscope.utils;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

public class LongLongRecordReader implements RecordReader<LongWritable,LongWritable> {
    LineRecordReader lineRecordReader;
    private LongWritable key = new LongWritable();
    private Text tmpValue = new Text();
    public LongLongRecordReader(Configuration job, FileSplit split, byte[] recordDelimiter)
        throws IOException {
        lineRecordReader = new LineRecordReader(job,split,recordDelimiter);
    }
    @Override
    public boolean next(LongWritable longWritable, LongWritable longWritable2) throws IOException {
        boolean res = lineRecordReader.next(key,tmpValue);
        if (!res) return false;
        longWritable.set(key.get());
        longWritable2.set(Long.parseLong(tmpValue.toString()));
        return true;
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public LongWritable createValue() {
        return new LongWritable();
    }

    @Override
    public long getPos() throws IOException {
        return lineRecordReader.getPos();
    }

    @Override
    public void close() throws IOException {
        lineRecordReader.close();
    }

    @Override
    public float getProgress() throws IOException {
        return lineRecordReader.getProgress();
    }
}
