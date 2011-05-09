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
