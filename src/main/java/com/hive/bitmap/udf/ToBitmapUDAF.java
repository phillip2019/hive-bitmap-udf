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
                    String rowString = PrimitiveObjectInspectorUtils.getString(p, inputOI);
                    BigInteger number = new BigInteger(rowString.replaceAll("-", ""), 16);
//                    long row = PrimitiveObjectInspectorUtils.getLong(p, inputOI);
//                    long row = PrimitiveObjectInspectorUtils.getLong(p, inputOI);
                    addBitmap(number, myAgg);
                } catch (NumberFormatException e) {
                    throw new HiveException(e);
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
            // 将 BigInteger 转换成字符串
            String stringValue = newRow.toString();

            // 将字符串转换成 long 类型
            long longValue = Long.parseLong(stringValue);
            myagg.bitmap.add(longValue);
        }
    }
}