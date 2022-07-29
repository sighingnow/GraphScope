package com.alibaba.graphscope.format;

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

public class LongLongRecordReader implements RecordReader<LongWritable, LongLong> {

    private Logger logger = LoggerFactory.getLogger(LongLongRecordReader.class.getName());
    private LineRecordReader lineRecordReader;
    //    private LongWritable key = new LongWritable();
    private Text text = new Text();

    //    private LongLong value = new LongLong();
    public LongLongRecordReader(Configuration job, FileSplit split, byte[] recordDelimiter)
        throws IOException {
        lineRecordReader = new LineRecordReader(job, split, recordDelimiter);
    }

    @Override
    public boolean next(LongWritable longWritable, LongLong longLong) throws IOException {
        boolean res = lineRecordReader.next(longWritable, text);
        if (!res) {
            return false;
        }
        Iterator<String> iter = Splitter.on(CharMatcher.whitespace()).split(text.toString())
            .iterator();
        longLong.first = Long.parseLong(iter.next());
        longLong.second = Long.parseLong(iter.next());
        return true;
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public LongLong createValue() {
        return new LongLong();
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
