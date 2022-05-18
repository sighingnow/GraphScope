package com.alibaba.graphscope.utils;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LongLongRecordReader implements RecordReader<LongWritable,LongWritable> {
    private Logger logger = LoggerFactory.getLogger(LongLongRecordReader.class.getName());
    LineRecordReader lineRecordReader;
    LongWritable key = new LongWritable();
    Text tmpValue = new Text();
    public LongLongRecordReader(Configuration job, FileSplit split, byte[] recordDelimiter)
        throws IOException {
        lineRecordReader = new LineRecordReader(job,split,recordDelimiter);
    }
    @Override
    public boolean next(LongWritable longWritable, LongWritable longWritable2) throws IOException {
        boolean res = lineRecordReader.next(key,tmpValue);
        logger.info("next line {}, {}", key, tmpValue);
        if (!res) return false;
        longWritable.set(key.get());
        String str = tmpValue.toString();
        Iterator<String> iter = Splitter.on(CharMatcher.breakingWhitespace()).split(str).iterator();
        iter.next();
        longWritable2.set(Long.parseLong(iter.next()));
        logger.info("parsed res: " + longWritable + ", " + longWritable2);
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
