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

package org.msgpack.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.io.Writable;
import org.msgpack.MessagePack;
import org.msgpack.MessagePackable;
import org.msgpack.unpacker.Unpacker;

/**
 * A Hadoop Writable wrapper for MessagePack (untyped).
 */
public class MessagePackWritable implements Writable {
	private MessagePackable payload = null;
	private MessagePack msgpack;

    public MessagePackWritable() {
    	msgpack = new MessagePack();
    }

    public MessagePackable getPayload(){
    	return payload;
    }

	public void setPayload(MessagePackable read) {
		payload = read;
	}

	public void readFields(DataInput dataInput) throws IOException {
		msgpack.createUnpacker(new DataInputWrapper(dataInput));
		payload.readFrom((Unpacker) dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		msgpack.createPacker(new DataOutputWrapper(dataOutput));
	}


	 
}
	


//*Stream and Data* are actually compatible
//if you just wrap them.
//I'm afraid I'm missing something here.
//hadoop is doing the same:
//org.apache.hadoop.hbase.io.DataInputInputStream 	
class DataInputWrapper extends InputStream {
	private DataInput dataInput;

	public DataInputWrapper(DataInput di) {
		dataInput = di;
	}

	@Override
	public int read() throws IOException {
		return dataInput.readByte();
	}

}

class DataOutputWrapper extends OutputStream{
	private DataOutput dataOutput;

	public DataOutputWrapper(DataOutput datOut) {
		dataOutput = datOut;
	}

	@Override
	public void write(int bt) throws IOException {
		dataOutput.write(bt);
	}
}


