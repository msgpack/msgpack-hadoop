package org.msgpack.hadoop.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.msgpack.hadoop.io.MessagePackWritable;
import org.msgpack.hadoop.mapreduce.input.MessagePackRecordReader;

public class MessagePackInputFormat extends FileInputFormat<LongWritable, MessagePackWritable> {
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<LongWritable, MessagePackWritable> createRecordReader(InputSplit split,
        TaskAttemptContext taskAttempt)
    throws IOException, InterruptedException {
        return new MessagePackRecordReader();
    }
}
