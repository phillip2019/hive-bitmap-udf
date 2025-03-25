// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.hive.bitmap.udf;

import com.hive.bitmap.common.BitmapUtil;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@Description(name = "bitmap_and", value = "a _FUNC_ b - Compute intersection of two or more input bitmaps, return the new bitmap")
public class BitmapAndUDF extends GenericUDF {
    public static final Logger logger = LoggerFactory.getLogger(ToBitmapUDAF.class);

    private transient BinaryObjectInspector inputOI0;
    private transient BinaryObjectInspector inputOI1;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentException("bitmap_and only takes 2 arguments");
        }
        ObjectInspector input0 = arguments[0];
        ObjectInspector input1 = arguments[1];
        if (!(input0 instanceof BinaryObjectInspector) || !(input1 instanceof BinaryObjectInspector)) {
            throw new UDFArgumentException("first and second argument must be a binary");
        }

        this.inputOI0 = (BinaryObjectInspector) input0;
        this.inputOI1 = (BinaryObjectInspector) input1;

        return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[]  args) throws HiveException {
        if (args[0] == null || args[1] == null) {
            return null;
        }
        byte[] inputBytes0 = this.inputOI0.getPrimitiveJavaObject(args[0].get());
        byte[] inputBytes1 = this.inputOI1.getPrimitiveJavaObject(args[1].get());

        try {
            Roaring64Bitmap bitmap0 = BitmapUtil.deserializeToBitmap(inputBytes0);
            Roaring64Bitmap bitmap1 = BitmapUtil.deserializeToBitmap(inputBytes1);
            bitmap0.and(bitmap1);
            return BitmapUtil.serializeToBytes(bitmap0);
        } catch (IOException ioException) {
            // 使用日志记录异常，而不是打印到标准错误流
            logger.error("执行bitmap_and操作时发生IO异常", ioException);
            throw new RuntimeException(ioException);
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "Usage: bitmap_and(bitmap1,bitmap2)";
    }
}