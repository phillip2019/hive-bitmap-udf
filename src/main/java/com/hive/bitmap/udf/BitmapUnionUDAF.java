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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.io.IOException;

import org.roaringbitmap.longlong.Roaring64Bitmap;
import com.hive.bitmap.common.BitmapUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * bitmap_union.
 *
 */
@Description(name = "bitmap_union", value = "_FUNC_(expr) - Calculate the grouped bitmap union , Returns an doris bitmap representation of a column.")
public class BitmapUnionUDAF extends AbstractGenericUDAFResolver {

    public static final Logger logger = LoggerFactory.getLogger(BitmapUnionUDAF.class);


    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }
        return new GenericEvaluate();
    }

    //The UDAF evaluator assumes that all rows it's evaluating have
    //the same (desired) value.
    public static class GenericEvaluate extends GenericUDAFEvaluator {

        private transient BinaryObjectInspector inputOI;
        private transient BinaryObjectInspector internalMergeOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            // init output object inspectors
            // The output of a partial aggregation is a binary
            if (m == Mode.PARTIAL1) {
                this.inputOI = (BinaryObjectInspector) parameters[0];
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

                // 检查当前模式和ObjectInspector
                if (this.inputOI == null) {
                    logger.error("BinaryObjectInspector (inputOI) is null, may be called in wrong mode");
                    // 尝试使用internalMergeOI作为后备
                    if (this.internalMergeOI != null) {
                        logger.warn("Falling back to internalMergeOI");
                        try {
                            byte[] partialResult = this.internalMergeOI.getPrimitiveJavaObject(p);
                            processPartialResult(myAgg, partialResult);
                        } catch (Exception e) {
                            logger.error("Error using fallback inspector: " + e.getMessage(), e);
                        }
                    }
                    return;
                }

                try {
                    byte[] partialResult = this.inputOI.getPrimitiveJavaObject(p);
                    processPartialResult(myAgg, partialResult);
                } catch (Exception e) {
                    logger.error("Error in iterate: " + e.getMessage(), e);
                }
            }
        }

        // 提取公共处理逻辑到一个单独的方法
        private void processPartialResult(BitmapAgg myAgg, byte[] partialResult) {
            if (partialResult != null && partialResult.length > 0) {
                try {
                    if (myAgg.bitmap == null) {
                        logger.warn("Bitmap in aggregation buffer is null, creating new bitmap");
                        myAgg.bitmap = new Roaring64Bitmap();
                    }

                    Roaring64Bitmap bitmap = BitmapUtil.deserializeToBitmap(partialResult);
                    if (bitmap != null) {
                        myAgg.bitmap.or(bitmap);
                    }
                } catch (IOException ioException) {
                    logger.error("Error processing bitmap: " + ioException.getMessage(), ioException);
                } catch (Exception e) {
                    logger.error("Unexpected error processing bitmap: " + e.getMessage(), e);
                }
            } else {
                logger.warn("Received null or empty bitmap data");
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
            if (partial == null) {
                logger.warn("Received null object in merge");
                return;
            }

            BitmapAgg myAgg = (BitmapAgg) agg;
            byte[] partialResult = this.internalMergeOI.getPrimitiveJavaObject(partial);

            if (partialResult == null || partialResult.length == 0) {
                logger.warn("Received null or empty byte array in merge");
                return;
            }

            try {
                Roaring64Bitmap bitmap = BitmapUtil.deserializeToBitmap(partialResult);
                myAgg.bitmap.or(bitmap);
            } catch (IOException e) {
                logger.error("Error deserializing bitmap in merge: " + e.getMessage(), e);
                throw new RuntimeException(e);
            } catch (Exception e) {
                logger.error("Unexpected error in bitmap merge: " + e.getMessage(), e);
                // 可以选择忽略此错误或抛出异常
                // throw new RuntimeException(e);
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) {
            return terminate(agg);
        }
    }
}
