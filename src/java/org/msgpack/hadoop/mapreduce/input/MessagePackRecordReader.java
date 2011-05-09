package org.msgpack.hadoop.mapreduce.input;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.LongWritable;

import org.msgpack.MessagePack;
import org.msgpack.Unpacker;
import org.msgpack.MessagePackObject;
import org.msgpack.hadoop.io.MessagePackWritable;

public class MessagePackRecordReader extends RecordReader<LongWritable, MessagePackWritable> {
    private Unpacker unpacker_;

    private final LongWritable key_ = new LongWritable(0);
    private final MessagePackWritable val_;

    protected long start_;
    protected long pos_;
    protected long end_;
    private FSDataInputStream fileIn_;

    public MessagePackRecordReader() {
        val_ = new MessagePackWritable();
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit)genericSplit;
        final Path file = split.getPath();
        Configuration conf = context.getConfiguration();

        // Open the file
        FileSystem fs = file.getFileSystem(conf);
        fileIn_ = fs.open(split.getPath());

        // Create streaming unpacker
        unpacker_ = new Unpacker(fileIn_);

        // Seek to the start of the split
        start_ = split.getStart();
        end_ = start_ + split.getLength();
        pos_ = start_;
    }

    @Override
    public float getProgress() {
        if (start_ == end_) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos_ - start_) / (float) (end_ - start_));
        }
    }

    @Override
    public synchronized void close() throws IOException {
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key_;
    }

    @Override
    public MessagePackWritable getCurrentValue() throws IOException, InterruptedException {
        return val_;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        for (MessagePackObject obj : unpacker_) {
            long key = fileIn_.getPos();
            MessagePackObject val = obj;
            key_.set(key);
            val_.set(val);
            return true;
        }
        return false;
    }
}
