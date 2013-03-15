/*
 * MessagePack-Hadoop Integration
 *
 * Copyright (C) 2009-2011 MessagePack Project
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.msgpack.hadoop.mapred;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.io.LongWritable;

import org.msgpack.MessagePack;
import org.msgpack.unpacker.Unpacker;
import org.msgpack.type.Value;
import org.msgpack.hadoop.io.MessagePackWritable;

public class MessagePackRecordReader implements RecordReader<LongWritable, MessagePackWritable> {
    private MessagePack msgPack_ = new MessagePack();
    private Unpacker unpacker_;

    protected long start_;
    protected long pos_;
    protected long end_;
    private FSDataInputStream fileIn_;

    public MessagePackRecordReader(InputSplit genericSplit, JobConf conf) throws IOException {
        FileSplit split = (FileSplit)genericSplit;
        final Path file = split.getPath();

        // Open the file
        FileSystem fs = file.getFileSystem(conf);
        fileIn_ = fs.open(split.getPath());

        // Create streaming unpacker
        unpacker_ = msgPack_.createUnpacker(fileIn_);

        // Seek to the start of the split
        start_ = split.getStart();
        end_ = start_ + split.getLength();
        pos_ = start_;
    }

    public float getProgress() {
        if (start_ == end_) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos_ - start_) / (float) (end_ - start_));
        }
    }

    public long getPos() {
        return pos_;
    }

    public synchronized void close() throws IOException {
    }

    public LongWritable createKey() {
        return new LongWritable();
    }

    public MessagePackWritable createValue() {
        return new MessagePackWritable();
    }

    public boolean next(LongWritable key, MessagePackWritable val)
    throws IOException  {
        for (Value obj : unpacker_) {
            pos_ = fileIn_.getPos();
            key.set(pos_);
            val.set(obj);
            return true;
        }
        return false;
    }
}
