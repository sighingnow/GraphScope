package com.alibaba.graphscope.utils;


import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.thirdparty.com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LongLongInputFormat extends FileInputFormat<LongWritable, LongLong> implements
    JobConfigurable {

    private Logger logger = LoggerFactory.getLogger(LongLongInputFormat.class.getName());
    private CompressionCodecFactory compressionCodecs = null;

    protected boolean isSplitable(FileSystem fs, Path file) {
        CompressionCodec codec = this.compressionCodecs.getCodec(file);
        return null == codec ? true : codec instanceof SplittableCompressionCodec;
    }

    public RecordReader<LongWritable, LongLong> getRecordReader(InputSplit genericSplit,
        JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(genericSplit.toString());
        String delimiter = job.get("textinputformat.record.delimiter");
        byte[] recordDelimiterBytes = null;
        if (null != delimiter) {
            recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
        }
        return new LongLongRecordReader(job, (FileSplit) genericSplit, recordDelimiterBytes);
    }

    @Override
    public void configure(JobConf jobConf) {
        this.compressionCodecs = new CompressionCodecFactory(jobConf);
    }
}
