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

package org.msgpack.hadoop.hive.serde2.lazy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.io.IOException;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;

import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import org.msgpack.hadoop.io.MessagePackWritable;

public class LazyMessagePackRow extends LazyStruct {
    private MessagePackWritable result_;
    private ArrayList<Object> cachedList_;

    public LazyMessagePackRow(LazySimpleStructObjectInspector oi) {
        super(oi);
    }

    public void init(MessagePackWritable r) {
        result_ = r;
        setParsed(false);
    }

    private void parse() {
        if (getFields() == null) {
            List<? extends StructField> fieldRefs = ((StructObjectInspector)getInspector()).getAllStructFieldRefs();
            setFields(new LazyObject[fieldRefs.size()]);
            for (int i = 0; i < getFields().length; i++) {
                getFields()[i] = LazyFactory.createLazyObject(fieldRefs.get(i).getFieldObjectInspector());
            }
            setFieldInited(new boolean[getFields().length]);
        }
        Arrays.fill(getFieldInited(), false);
        setParsed(true);
    }

    private Object uncheckedGetField(int fieldID) {
        if (!getFieldInited()[fieldID]) {
            getFieldInited()[fieldID] = true;

            ByteArrayRef ref = new ByteArrayRef();
            byte[] raw = result_.getRawBytes();
            ref.setData(raw);
            getFields()[fieldID].init(ref, 0, ref.getData().length);
        }

        return getFields()[fieldID].getObject();
    }

    @Override
    public Object getField(int fieldID) {
        if (!getParsed()) {
            parse();
        }
        return uncheckedGetField(fieldID);
    }

    @Override
    public ArrayList<Object> getFieldsAsList() {
        if (!getParsed()) {
            parse();
        }
        if (cachedList_ == null) {
            cachedList_ = new ArrayList<Object>();
        } else {
            cachedList_.clear();
        }
        for (int i = 0; i < getFields().length; i++) {
            cachedList_.add(uncheckedGetField(i));
        }
        return cachedList_;
    }

    @Override
    public Object getObject() {
        return this;
    }
}
