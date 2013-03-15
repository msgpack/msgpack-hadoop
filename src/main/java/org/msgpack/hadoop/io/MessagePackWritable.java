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

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;

import org.msgpack.MessagePack;
import org.msgpack.type.Value;

/**
 * A Hadoop Writable wrapper for MessagePack (untyped).
 */
public class MessagePackWritable implements WritableComparable<MessagePackWritable> {
    protected MessagePack msgPack = new MessagePack();
    protected Value val_= null;

    public MessagePackWritable() {}

    public MessagePackWritable(Value val) { val_ = val; }

    public void set(Value val) { val_ = val; }

    public Value get() { return val_; }

    public byte[] getRawBytes() {
        try {
            return msgPack.write(val_);
        } catch (IOException ioe) { return null; }
    }
    
    public void write(DataOutput out) throws IOException {
        assert(val_!= null);
        byte[] raw = getRawBytes();
        if (raw == null) return;
        out.writeInt(raw.length);
        out.write(raw, 0, raw.length);
    }

    @SuppressWarnings("unchecked")
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        if (size > 0) {
            byte[] raw = new byte[size];
            in.readFully(raw, 0, size);
            val_= msgPack.read(raw);
            assert(val_!= null);
        }
    }

    @Override
    public int compareTo(MessagePackWritable other) {
        byte[] raw1 = getRawBytes();
        byte[] raw2 = other.getRawBytes();
        return BytesWritable.Comparator.compareBytes(raw1, 0, raw1.length, raw2, 0, raw2.length);
    }
}
