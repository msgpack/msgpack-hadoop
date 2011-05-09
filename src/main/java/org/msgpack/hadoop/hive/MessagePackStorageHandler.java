package org.msgpack.hadoop.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;

import org.msgpack.hadoop.hive.serde2.MessagePackSerDe;
import org.msgpack.hadoop.mapred.MessagePackInputFormat;

class MessagePackStorageHandler extends DefaultStorageHandler {
    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return MessagePackInputFormat.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return null;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return MessagePackSerDe.class;
    }
}
