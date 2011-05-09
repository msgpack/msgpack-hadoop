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

package org.msgpack.hadoop.mapreduce.output;

import java.io.DataOutputStream;
import java.io.IOException;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.msgpack.MessagePack;
import org.msgpack.hadoop.io.MessagePackWritable;

public class MessagePackRecordWriter extends RecordWriter<NullWritable, MessagePackWritable> {
    protected final DataOutputStream out_;

    public MessagePackRecordWriter(DataOutputStream out) {
        out_ = out;
    }

    public void write(NullWritable key, MessagePackWritable val) throws IOException, InterruptedException {
        out_.write(val.getRawBytes());
    }

    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        out_.close();
    }
}
