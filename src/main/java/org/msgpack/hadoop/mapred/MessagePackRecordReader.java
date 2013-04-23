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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.msgpack.MessagePack;
import org.msgpack.MessagePackable;
import org.msgpack.hadoop.io.MessagePackWritable;

public class MessagePackRecordReader implements RecordReader<LongWritable, MessagePackWritable> {
	private static final Log LOG = LogFactory.getLog(MessagePackRecordReader.class);
    private org.msgpack.unpacker.Unpacker unpacker;

    protected long start;
    protected long pos;
    protected long end;
    private FSDataInputStream fileIn;

	private MessagePack msgpack;
	private Class<? extends MessagePackable> payloadClass;

    public MessagePackRecordReader(InputSplit genericSplit, JobConf conf, Class<? extends MessagePackable> payloadClass) throws IOException {

        this.payloadClass = payloadClass;
        
    	FileSplit split = (FileSplit)genericSplit;
        final Path file = split.getPath();
        
        // Open the file
        FileSystem fs = file.getFileSystem(conf);
        fileIn = fs.open(split.getPath());


        // Seek to the start of the split
        start = split.getStart();
        end = start + split.getLength();
        pos = start;
        
        // Create streaming unpacker
		msgpack = new MessagePack();
        unpacker = msgpack.createUnpacker(fileIn);
        
		if(LOG.isDebugEnabled())
			LOG.debug("Reading a split "+start+" to "+end);

        fileIn.seek(start);
    }

    public float getProgress() {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    public long getPos() {
        return pos;
    }

    public synchronized void close() throws IOException {
    	fileIn.close();
    }

    public LongWritable createKey() {
        return new LongWritable();
    }

    public MessagePackWritable createValue() {
        return new MessagePackWritable();
    }

    public boolean next(LongWritable key, MessagePackWritable val)
    throws IOException  {
		do{
    		try{
	            val.setPayload(unpacker.read(payloadClass));
	            pos = fileIn.getPos();
	            return true;
    		}catch(org.msgpack.MessageTypeException e){
    			pos++;
    			// unpacker isn't clean anymore, need a new one
    			unpacker = msgpack.createUnpacker(fileIn);
    			// the new unpacker seems to need this to read
    			// from where it should
    			fileIn.seek(pos);
        	}
		}while(pos < end);
        return false;
    }
}
