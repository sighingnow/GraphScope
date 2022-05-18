package com.alibaba.graphscope.utils;


import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LongLongInputFormat extends FileInputFormat<LongWritable, LongLong>{
    private Logger logger = LoggerFactory.getLogger(LongLongInputFormat.class.getName());

    @Override
    public RecordReader<LongWritable, LongLong> createRecordReader(InputSplit inputSplit,
        TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return null;
    }
}
