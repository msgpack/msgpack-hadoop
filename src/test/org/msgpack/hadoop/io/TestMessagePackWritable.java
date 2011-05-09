package org.msgpack.hadoop.io;

import java.util.*;
import java.io.*;

import org.apache.commons.codec.binary.Base64;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.msgpack.*;
import org.msgpack.Templates.*;
import org.msgpack.hadoop.io.MessagePackWritable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the MessagePackWritable class.
 */
public class TestMessagePackWritable extends TestCase {
    public void testMessagePackWritable() throws Exception {
        byte[] raw = MessagePack.pack(10);
        MessagePackObject obj = MessagePack.unpack(raw);
        MessagePackWritable r1 = new MessagePackWritable(obj);
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bo);
        r1.write(out);
        byte[] serialized = bo.toByteArray();

        MessagePackWritable r2 = new MessagePackWritable();
        ByteArrayInputStream bi = new ByteArrayInputStream(serialized);
        DataInputStream in = new DataInputStream(bi);
        r2.readFields(in);

        assertEquals(r1.get().convert(Templates.TLong),
                     r2.get().convert(Templates.TLong));
    }
}
