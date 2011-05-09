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

package org.msgpack.hadoop.hive.serde2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.io.*;            


import org.apache.commons.codec.binary.Base64;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.msgpack.*;
import org.msgpack.hadoop.io.MessagePackWritable;
import org.msgpack.hadoop.hive.serde2.MessagePackSerDe;

/**
 * Tests the MessagePackSerDe class.
 */
public class TestMessagePackSerDe extends TestCase {
    public void testMessagePackSerDe() throws Exception {
        // Create the SerDe
        MessagePackSerDe serDe = new MessagePackSerDe();
        Configuration conf = new Configuration();
        Properties tbl = createProperties();
        serDe.initialize(conf, tbl);

        byte[] raw = MessagePack.pack(10);
        MessagePackWritable r = new MessagePackWritable(MessagePack.unpack(raw));
        Object[] expectedFieldsData = {
            new Text(raw),
        };
        deserializeAndSerialize(serDe, r, expectedFieldsData);
    }

    private void deserializeAndSerialize(
        MessagePackSerDe serDe, MessagePackWritable r,
        Object[] expectedFieldsData) throws SerDeException {

        // Get the row structure
        StructObjectInspector oi = (StructObjectInspector)serDe.getObjectInspector();
        List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
        assertEquals(1, fieldRefs.size());

        // Deserialize
        Object row = serDe.deserialize(r);
        for (int i = 0; i < fieldRefs.size(); i++) {
            Object fieldData = oi.getStructFieldData(row, fieldRefs.get(i));
            if (fieldData != null) {
                fieldData = ((LazyPrimitive<?, ?>)fieldData).getWritableObject();
            }
            assertEquals("Field " + i, expectedFieldsData[i], fieldData);
        }
    }

    private Properties createProperties() {
        Properties tbl = new Properties();
        // Set the configuration parameters
        tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");
        tbl.setProperty("columns", "v");
        tbl.setProperty("columns.types", "string");
        return tbl;
    }
}
