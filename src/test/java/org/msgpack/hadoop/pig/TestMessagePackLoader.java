package org.msgpack.hadoop.pig;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import org.apache.pig.ExecType;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.pig.PigServer;
import org.apache.pig.tools.parameters.ParseException;
import org.apache.pig.test.Util;

import org.msgpack.MessagePack;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestMessagePackLoader {
    private static final String dataDir = "target/test/tmpdata/";
    private static final String singleFieldInput = "TestMessagePackLoader_single_field_input";
    private static final String arrayInput = "TestMessagePackLoader_array_input";
    private static final String mapInput = "TestMessagePackLoader_map_input";
    private static final String arrayOfMapsInput = "TestMessagePackLoader_array_of_maps_input";
    private static final String mapOfArraysInput = "TestMessagePackLoader_map_of_arrays_input";

    static PigServer pig;
    static MessagePack msgpack;

    // Pig test util has write String[], but not write byte[]
    private File createMsgpackTestFile(String filename, Object[] inputData) 
    throws IOException {
        File f = new File(filename);
        f.deleteOnExit();
        FileOutputStream fos = new FileOutputStream(f);
        for (Object o : inputData) {
            msgpack.write(fos, o);
        }
        fos.close();
        return f;
    }

    @Before
    public void setup() throws IOException {
        pig = new PigServer(ExecType.LOCAL);
        msgpack = new MessagePack();

        Util.deleteDirectory(new File(dataDir));
        try {
            pig.mkdirs(dataDir);

            createMsgpackTestFile(dataDir + singleFieldInput,
                new Object[] {
                    new Integer(1), new Integer(2),
                    new Integer(3), new Integer(4)
            });

            List<Object> arr = new ArrayList<Object>();
            arr.add(new Integer(1));
            arr.add(new Long(1234567890123L));
            arr.add(new Float(1.234f));
            arr.add(new Double(3.14159265359));
            arr.add("aardvark");
            arr.add((new DataByteArray("beryllium")).get());
            arr.add(null);
            createMsgpackTestFile(dataDir + arrayInput,
                new Object[] { arr }
            );

            Map<Object, Object> map = new HashMap<Object, Object>();
            map.put("i", new Integer(1));
            map.put("l", new Long(1234567890123L));
            map.put("f", new Float(1.234f));
            map.put("d", new Double(3.14159265359));
            map.put("c", "aardvark");
            map.put("b", (new DataByteArray("beryllium")).get());
            map.put("n", null);
            createMsgpackTestFile(dataDir + mapInput,
                new Object[] { map }
            );

            List<Object> arrOfMaps = new ArrayList<Object>();
            Map<Object, Object> m1 = new HashMap<Object, Object>();
            m1.put("i", new Integer(1));
            m1.put("f", new Float(1.234f));
            Map<Object, Object> m2 = new HashMap<Object, Object>();
            m2.put("l", new Long(1000000000000L));
            m2.put("d", new Double(3.14159265359));
            arrOfMaps.add(m1);
            arrOfMaps.add(m2);
            createMsgpackTestFile(dataDir + arrayOfMapsInput,
                new Object[] { arrOfMaps }
            );

            Map<Object, Object> mapOfArrs = new HashMap<Object, Object>();
            List<Object> a1 = new ArrayList<Object>();
            a1.add(new Integer(1));
            a1.add(new Float(1.234f));
            List<Object> a2 = new ArrayList<Object>();
            a2.add(new Long(1000000000000L));
            a2.add(new Double(3.14159265359));
            mapOfArrs.put("a1", a1);
            mapOfArrs.put("a2", a2);
            createMsgpackTestFile(dataDir + mapOfArraysInput,
                new Object[] { mapOfArrs }
            );
        } catch (IOException e) {};
    }

    @After
    public void cleanup() throws IOException {
        Util.deleteDirectory(new File(dataDir));
        pig.shutdown();
    }
    
    @Test
    public void singleField() throws IOException {
        String input = singleFieldInput;
        String schema = "i: int";

        pig.registerQuery(
            "data = load '" + dataDir + input + "' " +
            "using org.msgpack.hadoop.pig.MessagePackLoader('" + schema + "');"
        );

        Iterator<Tuple> data = pig.openIterator("data");
        String[] expected = {
            "(1)", "(2)",
            "(3)", "(4)"
        };
        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(data, "\n"));
    }

    @Test
    public void arrayAsBag() throws IOException {
        String input = arrayInput;
        String schema = "arr: {t: (b: bytearray)}";

        pig.registerQuery(
            "data = load '" + dataDir + input + "' " +
            "using org.msgpack.hadoop.pig.MessagePackLoader('" + schema + "');"
        );

        Iterator<Tuple> data = pig.openIterator("data");
        String[] expected = {
            "({(1),(1234567890123),(1.234),(3.14159265359),(aardvark),(beryllium),()})"
        };
        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(data, "\n"));
    }
    
    @Test
    public void arrayAsTuple() throws IOException {
        String input = arrayInput;
        String schema = "i: int, l: long, f: float, d: double, c: chararray, b: bytearray, n: bytearray";

        pig.registerQuery(
            "data = load '" + dataDir + input + "' " +
            "using org.msgpack.hadoop.pig.MessagePackLoader('" + schema + "');"
        );

        Iterator<Tuple> data = pig.openIterator("data");
        String[] expected = {
            "(1,1234567890123,1.234,3.14159265359,aardvark,beryllium,)"
        };
        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(data, "\n"));
    }
    
    @Test
    public void mapAsField() throws IOException {
        String input = mapInput;
        String schema = "m: [bytearray]";

        pig.registerQuery(
            "data = load '" + dataDir + input + "' " +
            "using org.msgpack.hadoop.pig.MessagePackLoader('" + schema + "');"
        );

        Iterator<Tuple> data = pig.openIterator("data");
        String[] expected = {
            "([f#1.234,d#3.14159265359,b#beryllium,c#aardvark,n#,l#1234567890123,i#1])"
        };
        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(data, "\n"));
    }
    
    @Test
    public void mapAsTuple() throws IOException {
        String input = mapInput;
        String schema = "i: int, l: long, f: float, d: double, c: chararray, b: bytearray, n: bytearray";

        pig.registerQuery(
            "data = load '" + dataDir + input + "' " +
            "using org.msgpack.hadoop.pig.MessagePackLoader('" + schema + "');"
        );

        Iterator<Tuple> data = pig.openIterator("data");
        String[] expected = {
            "(1,1234567890123,1.234,3.14159265359,aardvark,beryllium,)"
        };
        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(data, "\n"));
    }
    
    @Test
    public void arrayOfMaps() throws IOException {
        String input = arrayOfMapsInput;
        String schema = "arr: {t: (m: [bytearray])}";

        pig.registerQuery(
            "data = load '" + dataDir + input + "' " +
            "using org.msgpack.hadoop.pig.MessagePackLoader('" + schema + "');"
        );

        Iterator<Tuple> data = pig.openIterator("data");
        String[] expected = {
            "({([f#1.234,i#1]),([d#3.14159265359,l#1000000000000])})"
        };
        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(data, "\n"));
    }
    
    @Test
    public void mapOfArrays() throws IOException {
        String input = mapOfArraysInput;
        String schema = "m: [{t: (b: bytearray)}]";

        pig.registerQuery(
            "data = load '" + dataDir + input + "' " +
            "using org.msgpack.hadoop.pig.MessagePackLoader('" + schema + "');"
        );

        Iterator<Tuple> data = pig.openIterator("data");
        String[] expected = {
            "([a1#{(1),(1.234)},a2#{(1000000000000),(3.14159265359)}])"
        };
        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(data, "\n"));
    }
    
    @Test
    public void pushProjectionFromArray() throws IOException {
        String input = arrayInput;
        String schema = "i: int, l: long, f: float, d: double, c: chararray, b: bytearray, n: bytearray";

        pig.registerQuery(
            "data = load '" + dataDir + input + "' " +
            "using org.msgpack.hadoop.pig.MessagePackLoader('" + schema + "');"
        );
        pig.registerQuery("projected = FOREACH data GENERATE l, f, c;");

        Iterator<Tuple> projected = pig.openIterator("projected");
        String[] expected = {
            "(1234567890123,1.234,aardvark)"
        };
        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(projected, "\n"));
    }
    
    @Test
    public void pushProjectionFromMap() throws IOException {
        String input = mapInput;
        String schema = "i: int, l: long, f: float, d: double, c: chararray, b: bytearray, n: bytearray";

        pig.registerQuery(
            "data = load '" + dataDir + input + "' " +
            "using org.msgpack.hadoop.pig.MessagePackLoader('" + schema + "');"
        );
        pig.registerQuery("projected = FOREACH data GENERATE l, f, c;");

        Iterator<Tuple> projected = pig.openIterator("projected");
        String[] expected = {
            "(1234567890123,1.234,aardvark)"
        };
        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(projected, "\n"));
    }
    
    @Test
    public void inferSchemaFlatMap() throws IOException {
        String input = mapInput;

        pig.registerQuery(
            "data = load '" + dataDir + input + "' " +
            "using org.msgpack.hadoop.pig.MessagePackLoader();"
        );

        Iterator<Tuple> data = pig.openIterator("data");
        String[] expected = {
            "([f#1.234,d#3.14159265359,b#beryllium,c#aardvark,n#,l#1234567890123,i#1])"
        };
        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(data, "\n"));
    }
    
    @Test
    public void inferSchemaMapOfArrays() throws IOException {
        String input = mapOfArraysInput;

        pig.registerQuery(
            "data = load '" + dataDir + input + "' " +
            "using org.msgpack.hadoop.pig.MessagePackLoader();"
        );

        Iterator<Tuple> data = pig.openIterator("data");
        String[] expected = {
            "([a1#{(1),(1.234)},a2#{(1000000000000),(3.14159265359)}])"
        };
        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(data, "\n"));
    }
}