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
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.io.IOException;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.SerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.msgpack.hadoop.io.MessagePackWritable;
import org.msgpack.hadoop.hive.serde2.lazy.LazyMessagePackRow;

public class MessagePackSerDe implements SerDe {
    private static final Log LOG = LogFactory.getLog(MessagePackSerDe.class.getName());

    private SerDeParameters serdeParams_;
    private ObjectInspector cachedObjectInspector_;
    private LazyMessagePackRow cachedMessagePackRow_;

    @Override
    public void initialize(Configuration conf, Properties tbl) throws SerDeException {
        String serdeName = getClass().getName();
        serdeParams_ = LazySimpleSerDe.initSerdeParams(conf, tbl, serdeName);

        cachedObjectInspector_ = LazyFactory.createLazyStructInspector(
            serdeParams_.getColumnNames(),
            serdeParams_.getColumnTypes(),
            serdeParams_.getSeparators(),
            serdeParams_.getNullSequence(),
            serdeParams_.isLastColumnTakesRest(),
            serdeParams_.isEscaped(),
            serdeParams_.getEscapeChar());

        cachedMessagePackRow_ = new LazyMessagePackRow((LazySimpleStructObjectInspector)cachedObjectInspector_);
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return cachedObjectInspector_;
    }

    @Override
    public Object deserialize(Writable blob) throws SerDeException {
        if (!(blob instanceof MessagePackWritable)) {
            throw new SerDeException(getClass().toString()
                + ": expects either MessagePackWritable object!");
        }
        cachedMessagePackRow_.init((MessagePackWritable)blob);
        return cachedMessagePackRow_;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return MessagePackWritable.class;
    }

    @Override
    public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
        LOG.info(obj.toString());
        LOG.info(objInspector.toString());
        return null;
    }
}
