package org.msgpack.hadoop.hive.udf;

import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.commons.codec.binary.Base64;
import org.msgpack.MessagePackObject;
import org.msgpack.MessageTypeException;
import org.msgpack.MessagePack;
import static org.msgpack.Templates.*;

@Description(name = "msgpack_map",
    value = "_FUNC_(msgpackBinary, col1, col2, ..., colN) - parse MessagePack raw binary into a map. " +
    		"All the input parameters and output column types are string.")
public class GenericUDTFMessagePackMap extends GenericUDTF {

    private static Log LOG = LogFactory.getLog(GenericUDTFMessagePackMap.class.getName());

    int numCols;    // number of output columns
    String[] keys; // array of path expressions, each of which corresponds to a column
    Text[] retVals; // array of returned column values
    Text[] cols;    // object pool of non-null Text, avoid creating objects all the time
    Object[] nullVals; // array of null column values
    ObjectInspector[] inputOIs; // input ObjectInspectors
    boolean pathParsed = false;
    boolean seenErrors = false;

    @Override
    public void close() throws HiveException {
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args)
        throws UDFArgumentException {
        inputOIs = args;
        numCols = args.length - 1;

        if (numCols < 1) {
            throw new UDFArgumentException("msgpack_map() takes at least two arguments: " +
                                           "the MessagePack binary a key");
        }

        for (int i = 1; i < args.length; ++i) {
            // public static enum Category {
            //   PRIMITIVE, LIST, MAP, STRUCT, UNION
            // };
            if (args[i].getCategory() != ObjectInspector.Category.PRIMITIVE ||
                !args[i].getTypeName().equals(Constants.STRING_TYPE_NAME)) {
                throw new UDFArgumentException("msgpack_map()'s arguments have to be string type");
            }
        }

        seenErrors = false;
        pathParsed = false;
        keys = new String[numCols];
        cols = new Text[numCols];
        retVals = new Text[numCols];
        nullVals = new Object[numCols];

        for (int i = 0; i < numCols; ++i) {
            cols[i] = new Text();
            //retVals[i] = cols[i];
            nullVals[i] = null;
        }

        // construct output object inspector
        ArrayList<String> fieldNames = new ArrayList<String>(numCols);
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(numCols);
        for (int i = 0; i < numCols; ++i) {
            // column name can be anything since it will be named by UDTF as clause
            fieldNames.add("c" + i);
            // all returned type will be Text
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] o) throws HiveException {

        if (o[0] == null) {
            forward(nullVals);
            return;
        }
        // get the path expression for the 1st row only
        if (!pathParsed) {
            for (int i = 0;i < numCols; ++i) {
                keys[i] = ((StringObjectInspector) inputOIs[i+1]).getPrimitiveJavaObject(o[i+1]);
            }
            pathParsed = true;
        }

        byte[] binary = ((StringObjectInspector) inputOIs[0]).getPrimitiveWritableObject(o[0]).getBytes();
        if (binary == null) {
            forward(nullVals);
            return;
        }
        try {
            Map<String,MessagePackObject> map = (Map<String,MessagePackObject>)
                MessagePack.unpack(binary, tMap(TString,TAny));
            for (int i = 0; i < numCols; ++i) {
                MessagePackObject obj = map.get(keys[i]);
                if(obj == null) {
                    retVals[i] = null;
                } else {
                    retVals[i] = setText(cols[i], obj);
                }
            }
            //for (int i = 0; i < numCols; ++i) {
            //  if (jsonObj.isNull(keys[i])) {
            //    retVals[i] = null;
            //  } else {
            //    if (retVals[i] == null) {
            //      retVals[i] = cols[i]; // use the object pool rather than creating a new object
            //    }
            //    retVals[i].set(jsonObj.getString(keys[i]));
            //  }
            //}
            forward(retVals);
            return;

        } catch (MessageTypeException e) {
            // type error, object is not a map
            if (!seenErrors) {
                LOG.error("The input is not a map: " + e +  ". Skipping such error messages in the future.");
                seenErrors = true;
            }
            forward(nullVals);
            return;
        } catch (Exception e) {
            // parsing error, invalid MessagePack binary
            if (!seenErrors) {
                String base64 = new String(Base64.encodeBase64(binary));
                LOG.error("The input is not a valid MessagePack binary: " + base64 +  ". Skipping such error messages in the future.");
                seenErrors = true;
            }
            forward(nullVals);
            return;
        } catch (Throwable e) {
            LOG.error("MessagePack parsing/evaluation exception" + e);
            forward(nullVals);
            return;
        }
    }

    private Text setText(Text to, MessagePackObject obj) {
        if(obj.isBooleanType()) {
            if(obj.asBoolean()) {
                to.set("1");
            } else {
                to.set("0");
            }
            return to;

        } else if(obj.isIntegerType()) {
            to.set(Long.toString(obj.asLong()));
            return to;

        } else if(obj.isFloatType()) {
            to.set(Double.toString(obj.asDouble()));
            return to;

        } else if(obj.isArrayType()) {
            to.set(MessagePack.pack(obj));
            return to;

        } else if(obj.isMapType()) {
            to.set(MessagePack.pack(obj));
            return to;

        } else if(obj.isRawType()) {
            to.set(obj.asByteArray());
            return to;

        } else {
            return null;
        }
    }

    @Override
    public String toString() {
        return "msgpack_map";
    }
}
