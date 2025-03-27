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

package com.hive.bitmap.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.hive.bitmap.udf.ToBitmapUDAF;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BitmapUtil {
    public static final Logger logger = LoggerFactory.getLogger(BitmapUtil.class);

    public static byte[] serializeToBytes(Roaring64Bitmap bitmap) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); DataOutputStream dos = new DataOutputStream(bos)) {
            bitmap.serialize(dos);
            dos.flush();
            return bos.toByteArray();
        }
    }

    public static Roaring64Bitmap deserializeToBitmap(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            return new Roaring64Bitmap(); // 返回空位图而不是 null
        }

        try {
            Roaring64Bitmap bitmapValue = new Roaring64Bitmap();
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
                bitmapValue.deserialize(in);
                return bitmapValue;
            }
        } catch (Exception e) {
            // 记录错误并返回空位图
            logger.error("Error deserializing bitmap: {}", e.getMessage(), e);
            return new Roaring64Bitmap();
        }
    }
}