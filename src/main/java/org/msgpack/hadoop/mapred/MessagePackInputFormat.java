package org.msgpack.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileInputFormat;

import org.msgpack.hadoop.io.MessagePackWritable;
import org.msgpack.hadoop.mapred.MessagePackRecordReader;

public class MessagePackInputFormat extends FileInputFormat<LongWritable, MessagePackWritable> {
    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }

    @Override
    public RecordReader<LongWritable, MessagePackWritable> getRecordReader(InputSplit split,
        JobConf conf, Reporter reporter)
    throws IOException {
        return new MessagePackRecordReader(split, conf);
    }
}
