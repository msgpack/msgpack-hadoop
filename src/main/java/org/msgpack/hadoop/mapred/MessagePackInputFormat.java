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
