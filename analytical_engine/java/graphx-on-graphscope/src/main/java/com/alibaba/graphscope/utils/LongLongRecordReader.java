package com.alibaba.graphscope.utils;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LongLongRecordReader extends RecordReader<LongWritable, LongLong> {

    private Logger logger = LoggerFactory.getLogger(LongLongRecordReader.class.getName());
    private LineRecordReader lineRecordReader = new LineRecordReader();
    private LongLong value;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
        throws IOException, InterruptedException {
        lineRecordReader.initialize(inputSplit, taskAttemptContext);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return lineRecordReader.nextKeyValue();
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return lineRecordReader.getCurrentKey();
    }

    @Override
    public LongLong getCurrentValue() throws IOException, InterruptedException {
        String res = lineRecordReader.getCurrentValue().toString();
        Iterator<String> iter = Splitter.on(CharMatcher.breakingWhitespace()).split(res).iterator();
        value.first = Long.parseLong(iter.next());
        value.second = Long.parseLong(iter.next());
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return lineRecordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        lineRecordReader.close();
    }
}
