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

package org.msgpack.hadoop.mapreduce.input;

import java.io.EOFException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.msgpack.MessagePack;
import org.msgpack.hadoop.io.MessagePackWritable;
import org.msgpack.unpacker.SizeLimitException;
import org.msgpack.unpacker.Unpacker;
import org.msgpack.MessagePackable;

public class MessagePackRecordReader extends RecordReader<LongWritable, MessagePackWritable> {
	private static final Log LOG = LogFactory.getLog(MessagePackRecordReader.class);
    private Unpacker unpacker;

    private final LongWritable key = new LongWritable(0);
    private MessagePackWritable val;

    protected long start;
    protected long pos;
    protected long end;
    private long totalSkippedBytes;
    private FSDataInputStream fileIn;

	private MessagePack msgpack;
	private Class<? extends MessagePackable> payloadClass;

    public MessagePackRecordReader(MessagePackWritable writable, Class<? extends MessagePackable> payloadClass) {
        val = writable;
        this.payloadClass = payloadClass;
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit)genericSplit;
        final Path file = split.getPath();
        Configuration conf = context.getConfiguration();

        // Open the file
        FileSystem fs = file.getFileSystem(conf);
        fileIn = fs.open(split.getPath());

        totalSkippedBytes = 0;
        // Seek to the start of the split
        start = split.getStart();
        end = start + split.getLength();
        pos = start;
        
        // Create streaming unpacker
		msgpack = new MessagePack();
        unpacker = msgpack.createUnpacker(fileIn);
        
		if(LOG.isTraceEnabled())
			LOG.trace("Reading a split "+start+" to "+end+".");

        fileIn.seek(start);

    }

    @Override
    public float getProgress() {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    @Override
    public synchronized void close() throws IOException {
        LOG.debug("In total, skipped "+totalSkippedBytes+" bytes on this split.");
    	fileIn.close();
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public MessagePackWritable getCurrentValue() throws IOException, InterruptedException {
        return val;
    }
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {	
    	
    	if(pos < end ){
    		try{
    			long theKey = pos;
        		if(readAValue()){
        			key.set(theKey);
        			return true;
        		}
        	}catch(EOFException e){
    			if(LOG.isDebugEnabled())
    				LOG.debug("Reached end of file while reading msgpack data.");
        	}
    	}
    	
        return false;
    }

    private void skipReading() throws IOException{
		pos++;
		// unpacker isn't clean anymore, need a new one
		unpacker = msgpack.createUnpacker(fileIn);
		// the new unpacker seems to need this to read
		// from where it should
		fileIn.seek(pos);
    }
    
    // returns true if value could be read successfully
	private boolean readAValue() throws IOException{
		long countSkip = 0;
        long lastCounted = pos;
        boolean successfulRead = false;
		// try reading an object. If it fails, skip a byte
		do{
    		try{
	            val.setPayload(unpacker.read(payloadClass));
	            pos = fileIn.getPos();
                successfulRead = true;
                continue;
    		}catch(org.msgpack.MessageTypeException e){
                // skip reading is done below
    		}catch(SizeLimitException e){
        		LOG.warn("SizeLimitException while parsing msgpack message: "+e.getMessage());
        	}catch(java.io.IOException e){
        		// this is ugly: we are parsing an IO exception to make sure it was thrown by msgpack itself
        		// The real problem is that msgpack 0.6.7 throws an IO exception
        		// when it should throw a FormatException (see org.msgpack.unpacker.MessagePackUnpacker:323)
        		LOG.warn("IOException.");
        		if(null != e.getMessage() && e.getMessage().startsWith("Invalid byte: ")){
        			// thrown by MsgPack (hopefully :s)
        			LOG.warn("Error while parsing msgpack message: "+e.getMessage());
        		}else{
                    LOG.warn("IOException while parsing msgpack message: "+e.getMessage());
        			throw e;
        		}
        	}
            
            // if try succeeded, we returned
            // if error was caught, we end up here
            skipReading();
            countSkip += pos - lastCounted + 1;
            lastCounted = pos;
            
		}while(!successfulRead && pos < end);
		
		// Debugging
		if(LOG.isTraceEnabled() && countSkip > 0){
            LOG.trace("Skipped "+countSkip+" bytes. Pos:"+pos);
            if( pos >= end ){
                LOG.trace("Reached end of split while skipping bytes. Pos:"+pos);
            }
        }
        
        totalSkippedBytes += countSkip;
        
		return successfulRead;
	}
}
