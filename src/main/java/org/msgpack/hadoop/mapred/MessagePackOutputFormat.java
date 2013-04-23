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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.msgpack.hadoop.io.MessagePackWritable;

public class MessagePackOutputFormat extends FileOutputFormat<NullWritable, MessagePackWritable> {
    @Override
    public RecordWriter<NullWritable, MessagePackWritable> getRecordWriter(FileSystem ignored,
        JobConf job, String name, Progressable progress)
    throws IOException {
        Path file = FileOutputFormat.getTaskOutputPath(job, name);
        FileSystem fs = file.getFileSystem(job);
        FSDataOutputStream fileOut = fs.create(file, progress);
        return new MessagePackRecordWriter(fileOut);
    }
}
