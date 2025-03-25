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
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;

/**
 * ToBitmap.
 *
 */
@Description(name = "to_bitmap", value = "_FUNC_(expr) - Returns an doris bitmap representation of a column.")
public class ToBitmapUDAF extends AbstractGenericUDAFResolver {
    public static final Logger logger = LoggerFactory.getLogger(ToBitmapUDAF.class);

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }
        return new GenericEvaluate();
    }

    /**
     * The UDAF evaluator assumes that all rows it's evaluating have
     * the same (desired) value.
     **/
    public static class GenericEvaluate extends GenericUDAFEvaluator {

        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
        private PrimitiveObjectInspector inputOI;

        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations
        // (doris bitmaps)

        private transient BinaryObjectInspector internalMergeOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            // init output object inspectors
            // The output of a partial aggregation is a binary
            if (m == Mode.PARTIAL1) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                this.internalMergeOI = (BinaryObjectInspector) parameters[0];
            }
            return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
        }

        /** class for storing the current partial result aggregation */
        @AggregationType(estimable = true)
        static class BitmapAgg extends AbstractAggregationBuffer {
            Roaring64Bitmap bitmap;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((BitmapAgg) agg).bitmap = new Roaring64Bitmap();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            BitmapAgg result = new BitmapAgg();
            reset(result);
            return result;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            Object p = parameters[0];
            if (p != null) {
                BitmapAgg myAgg = (BitmapAgg) agg;
                logger.info("source data type: {}", inputOI.getTypeName());
                try {
                    // 根据输入类型选择适当的处理方式
                    if (inputOI.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
                        // 处理字符串输入，假设是十六进制格式
                        String rowString = PrimitiveObjectInspectorUtils.getString(p, inputOI);
                        if (rowString.startsWith("0x") || rowString.startsWith("0X")) {
                            rowString = rowString.substring(2);
                        }
                        try {
                            // 尝试作为十六进制处理
                            BigInteger number = new BigInteger(rowString.replaceAll("-", ""), 16);
                            addBitmap(number, myAgg);
                        } catch (NumberFormatException e) {
                            // 如果不是有效的十六进制，尝试作为十进制处理
                            try {
                                long value = Long.parseLong(rowString);
                                addBitmap(value, myAgg);
                            } catch (NumberFormatException ex) {
                                throw new HiveException("无法将输入值 '" + rowString + "' 转换为数字", ex);
                            }
                        }
                    } else {
                        // 对于数值类型，直接获取long值
                        long row = PrimitiveObjectInspectorUtils.getLong(p, inputOI);
                        addBitmap(row, myAgg);
                    }
                } catch (NumberFormatException e) {
                    throw new HiveException("输入值转换为数字时出错", e);
                }
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) {
            BitmapAgg myAgg = (BitmapAgg) agg;
            try {
                return BitmapUtil.serializeToBytes(myAgg.bitmap);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) {
            BitmapAgg myAgg = (BitmapAgg) agg;
            byte[] partialResult = this.internalMergeOI.getPrimitiveJavaObject(partial);
            try {
                myAgg.bitmap.or(BitmapUtil.deserializeToBitmap(partialResult));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) {
            return terminate(agg);
        }

        private void addBitmap(long newRow, BitmapAgg myagg) {
            myagg.bitmap.add(newRow);
        }

        private void addBitmap(BigInteger newRow, BitmapAgg myagg) {
            // 检查是否超出long的范围
            if (newRow.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0 ||
                    newRow.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0) {
                logger.warn("BigInteger值 {} 超出long范围，将被截断", newRow);
                // 可以选择跳过、截断或者采取其他处理策略
            }

            try {
                long longValue = newRow.longValue(); // 使用longValue()方法而不是通过字符串转换
                myagg.bitmap.add(longValue);
            } catch (Exception e) {
                logger.error("将BigInteger转换为long时出错: {}", e.getMessage());
                // 可以选择忽略此错误或者采取其他处理策略
            }
        }
    }

    public static void main(String[] args) {
        // 创建一个 Roaring64Bitmap
        Roaring64Bitmap bitmap = new Roaring64Bitmap();

        // 将 BigInteger 类型塞入 Roaring64Bitmap 中
        BigInteger bigInteger = new BigInteger("18a462abbc3cf-0e95bcb772f8c4-61251c4d-313664-18a462abbc5170".replaceAll("-", ""), 16);

        // 将 BigInteger 转换成字符串
        String stringValue = bigInteger.toString();

        // 将字符串转换成 long 类型
        long longValue = Long.parseLong(stringValue);

        // 将 long 值塞入 Roaring64Bitmap 中
        bitmap.add(longValue);

        // 输出 Roaring64Bitmap
        System.out.println(bitmap);
    }
}