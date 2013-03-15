/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.msgpack.hadoop.pig;

import java.io.IOException;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPushDown;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

import org.msgpack.MessagePack;
import org.msgpack.type.ArrayValue;
import org.msgpack.type.BooleanValue;
import org.msgpack.type.FloatValue;
import org.msgpack.type.IntegerValue;
import org.msgpack.type.MapValue;
import org.msgpack.type.RawValue;
import org.msgpack.type.Value;
import org.msgpack.type.ValueType;

import org.msgpack.hadoop.mapreduce.input.MessagePackInputFormat;
import org.msgpack.hadoop.mapreduce.input.MessagePackRecordReader;

public class MessagePackLoader extends LoadFunc implements LoadMetadata, LoadPushDown {
    private String udfContextSignature = null;

    private MessagePack msgpack = new MessagePack();
    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory bagFactory = BagFactory.getInstance();

    protected MessagePackRecordReader reader = null;
    protected String escapedSchemaStr = null;
    protected ResourceSchema schema = null;
    protected ResourceFieldSchema[] fields = null;
    protected boolean inferringSchema = false;
    
    private boolean[] requiredFields = null;

    public static final String DEFAULT_MAP_NAME = "object";
    private static final String SCHEMA_SIGNATURE = "pig.messagepackloader.schema";
    private static final String REQUIRED_FIELDS_SIGNATURE = "pig.messagepackloader.required_fields";
    private static final Log log = LogFactory.getLog(MessagePackLoader.class);

    public MessagePackLoader() {
        try {
            // If no schema is passed in, load the whole document as a map.
            escapedSchemaStr = DEFAULT_MAP_NAME + ":[bytearray]";
            schema = new ResourceSchema(Utils.getSchemaFromString(escapedSchemaStr));
            fields = schema.getFields();
            inferringSchema = true;
        } catch (ParserException e) {
            // Should never get here
            throw new IllegalArgumentException("Error constructing default MessagePackLoader schema");
        }
    }

    public MessagePackLoader(String schemaStr) {
        escapedSchemaStr = escapeSchemaStr(schemaStr);
        try {
            schema = new ResourceSchema(Utils.getSchemaFromString(escapedSchemaStr));
            fields = schema.getFields();
        } catch (ParserException pe) {
            throw new IllegalArgumentException("Invalid schema format: " + pe.getMessage());
        }
    }

    public String escapeSchemaStr(String schemaStr) {
        schemaStr = schemaStr.replaceAll("[\\r\\n]", "");
        String[] fieldSchemaStrs = schemaStr.split(",");
        StringBuilder escapedSchemaBuilder = new StringBuilder();

        for (int i = 0; i < fieldSchemaStrs.length; i++) {
            escapedSchemaBuilder.append(escapeFieldSchemaStr(fieldSchemaStrs[i]));
            if (i != fieldSchemaStrs.length - 1)
                escapedSchemaBuilder.append(",");
        }

        return escapedSchemaBuilder.toString();
    }

    // escape field names starting with underscores
    // ex. "__field: int" -> "u__field: int"
    // escape whitespace as directed, 
    // ex. "field\\ with\\ space: int" (Pig) -> "field\ with \ space: int" (fieldSchemaStr) -> "field_with_space: int" (escapedStr)
    private String escapeFieldSchemaStr(String fieldSchemaStr) {
        String escaped = fieldSchemaStr.trim().replaceAll("\\\\\\s+", "_");
        if (escaped.charAt(0) == '_') {
            escaped = "underscore" + escaped;
        }
        return escaped;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        final MessagePackInputFormat inputFormat = new MessagePackInputFormat();
        return inputFormat;
    }

    @Override
    public void setUDFContextSignature(String signature ) {
        udfContextSignature = signature;
    }

    public ResourceSchema getSchema(String location, Job job)
            throws IOException {

        if (schema != null) {
            // Send schema to backend
            // Schema should have been passed as an argument (-> constructor)
            // or provided in the default constructor

            UDFContext udfc = UDFContext.getUDFContext();
            Properties p = udfc.getUDFProperties(this.getClass(), new String[]{ udfContextSignature });
            p.setProperty(SCHEMA_SIGNATURE, escapedSchemaStr);

            return schema;
           } else {
            // Should never get here
            throw new IllegalArgumentException(
                "No schema found: default schema was never created and no user-specified schema was found."
            );
        }
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split)
            throws IOException {

        // Save reader to use in getNext()
        this.reader = (MessagePackRecordReader) reader;

        // Get schema from front-end
        UDFContext udfc = UDFContext.getUDFContext();
        Properties p = udfc.getUDFProperties(this.getClass(), new String[] { udfContextSignature });
        String schemaStr = p.getProperty(SCHEMA_SIGNATURE);

        if (schemaStr == null) {
            throw new IOException("Could not find schema in UDF context");
        }
        schema = new ResourceSchema(Utils.getSchemaFromString(schemaStr));

        String requiredFieldsStr = p.getProperty(REQUIRED_FIELDS_SIGNATURE);
        if (requiredFieldsStr != null) {
            requiredFields = (boolean[]) ObjectSerializer.deserialize(requiredFieldsStr);
        }
    }
    
    @Override
    public Tuple getNext() throws IOException {
        Value val = null;
        try {
            if (!reader.nextKeyValue()) return null;
            val = reader.getCurrentValue().get();
        } catch (Exception e) {
            throw new IOException(e);
        }

        Tuple t;
        if (val.isArrayValue()) {
            if (fields.length == 1 && fields[0].getType() == DataType.BAG) {
                // [1,2,3] -> ({(1), (2), (3)})
                t = tupleFactory.newTuple(readArrayAsBag(val.asArrayValue(), fields[0]));
            } else if (fields.length == 1 && fields[0].getType() == DataType.MAP) {
                throw new IOException(
                    "Can only infer schemas from msgpack MAP objects (found an ARRAY object). " +
                    "Please explicitly specify a schema in the MessagePackLoader constructor."
                );
            } else {
                // [1,2,3] -> (1,2,3)
                t = readArrayAsTuple(val.asArrayValue(), fields);
            }
        } else if (val.isMapValue()) {
            if (fields.length == 1 && fields[0].getType() == DataType.MAP) {
                // {"a":1,"b":2,"c":3} -> (["a"#1,"b"#2,"c"#3])
                t = tupleFactory.newTuple(readField(val, fields[0]));
            } else {
                // {"a":1,"b":2,"c":3} -> (1,2,3) iff schema is 'a,b,c'
                t = readMapAsTuple(val.asMapValue(), fields);
            }
        } else {
            // 1 -> (1) iff schema is a single field
            if (fields.length > 1) {
                throw new IOException(
                    "Schema calls for multiple fields," +
                    "but found a single field of MessagePack ValueType " + msgpackTypeName(val)
                );
            }
            if (!inferringSchema) {
                ResourceFieldSchema fs = fields[0];
                t = tupleFactory.newTuple(readField(val, fs));
            } else {
                throw new IOException(
                    "Can only infer schemas from msgpack MAP objects (found " + msgpackTypeName(val) + " object). " +
                    "Please explicitly specify a schema in the MessagePackLoader constructor."
                );
            }
        }

        return t;
    }

    private DataBag readArrayAsBag(ArrayValue val, ResourceFieldSchema fs)
                                   throws IOException {
        // drill down to tuple schema
        ResourceSchema nestedSchema = null;
        ResourceFieldSchema nestedFieldSchema = null;
        try {
            nestedSchema = fs.getSchema();
            nestedFieldSchema = nestedSchema.getFields()[0];
            nestedSchema = nestedFieldSchema.getSchema();
            nestedFieldSchema = nestedSchema.getFields()[0];
        } catch (Exception e) {
            nestedFieldSchema = new ResourceFieldSchema(new Schema.FieldSchema("item", DataType.BYTEARRAY));
        }

        DataBag bag = bagFactory.newDefaultBag();
        for (Value v : val) {
            bag.add(tupleFactory.newTuple(readField(v, nestedFieldSchema)));
        }
        return bag;
    }

    private Tuple readArrayAsTuple(ArrayValue val, ResourceFieldSchema[] fields)
                                   throws IOException {

        int size = getRequestedTupleSize();
        Tuple t = tupleFactory.newTuple(size);
        int i = 0;
        int tuplePos = 0;

        for (Value v : val) {
            if (requiredFields == null || requiredFields[i]) {
                t.set(tuplePos, readField(v, fields[i]));
                if (tuplePos >= size)
                    break;
                else
                    tuplePos++;
            }
            i++;
        }

        return t;
    }

    private Map<String, Object> readMapAsMap(MapValue val, ResourceFieldSchema fs)
                                             throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        for (Map.Entry<Value, Value> e : val.entrySet()) {
            String key = e.getKey().toString();
            if (e.getKey().isRawValue()) {
                // stupid msgpack toString() impl puts quotes around the actual value
                key = key.substring(1, key.length()-1);
            }
            Object entryVal = readField(e.getValue(), fs);
            map.put(key, entryVal);
        }
        return map;
    }

    private Tuple readMapAsTuple(MapValue val, ResourceFieldSchema[] fields)
                                 throws IOException {
        Map<String, Integer> fieldPositionMap = new HashMap<String, Integer>();
        Map<String, ResourceFieldSchema> fieldSchemaMap = new HashMap<String, ResourceFieldSchema>();
        int count = 0;
        for (int i = 0; i < fields.length; i++) {
            if (requiredFields == null || requiredFields[i]) {
                fieldPositionMap.put(fields[i].getName(), count);
                fieldSchemaMap.put(fields[i].getName(), fields[i]);
                count++;
            }
        }

        Tuple t = tupleFactory.newTuple(getRequestedTupleSize());
        for (Map.Entry<Value, Value> e : val.entrySet()) {
            String key = e.getKey().toString();
            if (e.getKey().isRawValue()) {
                // stupid msgpack toString() impl puts quotes around the actual value
                key = key.substring(1, key.length()-1);
            }
            if (fieldPositionMap.containsKey(key)) {
                int i = fieldPositionMap.get(key);
                ResourceFieldSchema fs = fieldSchemaMap.get(key);
                t.set(i, readField(e.getValue(), fs));
            }
        }
        return t;
    }

    private int getRequestedTupleSize() {
        int size;
        if (requiredFields == null)
            size = fields.length;
        else {
            size = 0;
            for (boolean b : requiredFields) {
                if (b) { size++; }
            }
        }
        return size;
    }

    private Object readField(Value val, ResourceFieldSchema fs)
                             throws IOException {
        
        ValueType valType = val.getType();
        byte fsType = fs.getType();

        if (valType == ValueType.ARRAY) {
            if (fsType == DataType.BAG) {
                return readArrayAsBag(val.asArrayValue(), fs);
            } else if (fsType == DataType.TUPLE) {
                try {
                    ResourceSchema nestedSchema = fs.getSchema();
                    ResourceFieldSchema[] nestedFields = nestedSchema.getFields();
                    return readArrayAsTuple(val.asArrayValue(), nestedFields);
                } catch (NullPointerException npe) {
                    throw new IOException(
                        "Nested tuple schema for field " + fs.getName() + "not fully specified."
                    );
                }
            } else if (fsType == DataType.BYTEARRAY && inferringSchema) {
                ResourceFieldSchema inferredFS = 
                    (new ResourceSchema(Utils.getSchemaFromString("b: {t: (b: bytearray)}"))).getFields()[0];
                return readArrayAsBag(val.asArrayValue(), inferredFS);
            } else {  valDoesNotMatchSchemaError(val, fs); }
        }
        else if (valType == ValueType.MAP) { 
            if (fsType == DataType.MAP) {
                ResourceSchema nestedSchema = fs.getSchema();
                if (nestedSchema == null) {
                   nestedSchema = new ResourceSchema(Utils.getSchemaFromString("b: bytearray"));
                }
                ResourceFieldSchema nestedField = nestedSchema.getFields()[0];
                return readMapAsMap(val.asMapValue(), nestedField);
            } else if (fsType == DataType.TUPLE) {
                try {
                    ResourceSchema nestedSchema = fs.getSchema();
                    ResourceFieldSchema[] nestedFields = nestedSchema.getFields();
                    return readMapAsTuple(val.asMapValue(), nestedFields);
                } catch (NullPointerException npe) {
                    throw new IOException(
                        "Nested tuple schema for field " + fs.getName() + "not fully specified."
                    );
                }
            } else if (fsType == DataType.BYTEARRAY && inferringSchema) {
                return readMapAsMap(val.asMapValue(), fs);
            } else { valDoesNotMatchSchemaError(val, fs); }
        }
        else if (valType == ValueType.INTEGER) { 
            IntegerValue integerVal = val.asIntegerValue();
            switch (fsType) {
                case DataType.INTEGER:
                    return new Integer(integerVal.getInt());
                case DataType.LONG:
                    return new Long(integerVal.getLong());
                case DataType.CHARARRAY:
                    return Long.toString(integerVal.getLong());
                case DataType.BYTEARRAY:
                    return new DataByteArray(integerVal.toString().getBytes("UTF-8"));
                case DataType.FLOAT:
                    return new Float((float) integerVal.getLong());
                case DataType.DOUBLE:
                    return new Double((double) integerVal.getLong());
                default:
                    valDoesNotMatchSchemaError(val, fs);
            }
        }
        else if (valType == ValueType.FLOAT) { 
            FloatValue floatVal = val.asFloatValue();
            switch (fsType) {
                case DataType.FLOAT:
                    return new Float(floatVal.getFloat());
                case DataType.DOUBLE:
                    return new Double(floatVal.getDouble());
                case DataType.CHARARRAY:
                    return Double.toString(floatVal.getDouble());
                case DataType.BYTEARRAY:
                    return new DataByteArray(floatVal.toString().getBytes("UTF-8"));
                case DataType.INTEGER:
                    return new Integer((int) floatVal.getFloat());
                case DataType.LONG:
                    return new Long((long) floatVal.getDouble());
                default:
                    valDoesNotMatchSchemaError(val, fs);
            }
        }
        else if (valType == ValueType.BOOLEAN) { 
            boolean boolVal = val.asBooleanValue().getBoolean();
            switch (fsType) {
                case DataType.INTEGER:
                    if (boolVal) { return new Integer(1); } else { return new Integer(0); } // oppan c-style
                case DataType.LONG:
                    if (boolVal) { return new Long(1); } else { return new Long(0); }
                case DataType.CHARARRAY:
                    return Boolean.toString(boolVal);
                case DataType.BYTEARRAY:
                    return new DataByteArray(Boolean.toString(boolVal).getBytes("UTF-8"));
                case DataType.FLOAT:
                    if (boolVal) { return new Float(1.0f); } else { return new Float(0.0f); }
                case DataType.DOUBLE:
                    if (boolVal) { return new Double(1.0); } else { return new Double(0.0); }
                default:
                    valDoesNotMatchSchemaError(val, fs);
            }
        }
        else if (valType == ValueType.RAW) {
            RawValue rawValue = val.asRawValue();
            String text = rawValue.getString();
            switch (fsType) {
                case DataType.CHARARRAY:
                    return text;
                case DataType.BYTEARRAY:
                    return new DataByteArray(text.getBytes("UTF-8"));
                default:
                    try {
                        switch (fsType) {
                            case DataType.INTEGER:
                                return Integer.parseInt(text);
                            case DataType.LONG:
                                return Long.parseLong(text);
                            case DataType.FLOAT:
                                return Float.parseFloat(text);
                            case DataType.DOUBLE:
                                return Double.parseDouble(text);
                            default:
                                valDoesNotMatchSchemaError(val, fs);
                        }
                    } catch (NumberFormatException nfe) {
                        throw new IOException(
                            "Field " + fs.getName() + " calls for type " + 
                            DataType.findTypeName(fs.getType()) + "; found msgpack ValueType RAW " +
                            "and could not parse into the requested type."
                        );
                    }
            }
        }
        else if (valType == ValueType.NIL) { return null; }
        throw new IOException("Unknown msgpack ValueType found (???)");
    }

    private void valDoesNotMatchSchemaError(Value val, ResourceFieldSchema fs)
                                            throws IOException {
        throw new IOException(
            "Expecting type " + DataType.findTypeName(fs.getType()) +
            " for field " + fs.getName() + " but found msgpack type " + msgpackTypeName(val)
        );
    }

    private String msgpackTypeName(Value val) {
        ValueType type = val.getType();
        if (type == ValueType.ARRAY) { return "ARRAY"; }
        else if (type == ValueType.MAP) { return "MAP"; }
        else if (type == ValueType.INTEGER) { return "INTEGER"; }
        else if (type == ValueType.FLOAT) { return "FLOAT"; }
        else if (type == ValueType.BOOLEAN) { return "BOOLEAN"; }
        else if (type == ValueType.RAW) { return "RAW"; }
        else if (type == ValueType.NIL) { return "NIL"; }
        else { return "UNKNOWN"; }
    }

    @Override
    public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList) throws FrontendException {
        if (requiredFieldList == null)
            return null;

        if (fields != null && requiredFieldList.getFields() != null)
        {
            requiredFields = new boolean[fields.length];

            for (RequiredField f : requiredFieldList.getFields()) {
                requiredFields[f.getIndex()] = true;
            }

            UDFContext udfc = UDFContext.getUDFContext();
            Properties p = udfc.getUDFProperties(this.getClass(), new String[]{ udfContextSignature });
            try {
                p.setProperty(REQUIRED_FIELDS_SIGNATURE, ObjectSerializer.serialize(requiredFields));
            } catch (Exception e) {
                throw new RuntimeException("Cannot serialize requiredFields for pushProjection");
            }
        }

        return new RequiredFieldResponse(true);
    }

    @Override
    public List<OperatorSet> getFeatures() {
        return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
    }

    public ResourceStatistics getStatistics(String location, Job job)
            throws IOException {
        // Not implemented
        return null;
    }

    public String[] getPartitionKeys(String location, Job job)
            throws IOException {
        // Not implemented
        return null;
    }

    public void setPartitionFilter(Expression partitionFilter)
            throws IOException {
        // Not implemented
    }
}
