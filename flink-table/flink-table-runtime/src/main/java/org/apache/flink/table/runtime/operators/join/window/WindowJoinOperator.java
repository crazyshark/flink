/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.join.window;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.operators.join.JoinConditionWithNullFilters;
import org.apache.flink.table.runtime.operators.window.slicing.WindowTimerService;
import org.apache.flink.table.runtime.operators.window.slicing.WindowTimerServiceImpl;
import org.apache.flink.table.runtime.operators.window.state.WindowListState;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;

import java.time.ZoneId;
import java.util.IdentityHashMap;
import java.util.List;

import static org.apache.flink.table.runtime.util.TimeWindowUtil.isWindowFired;

/**
 * Streaming window join operator.
 *
 * <p>Note: currently, {@link WindowJoinOperator} doesn't support early-fire and late-arrival. Thus
 * late elements (elements belong to emitted windows) will be simply dropped.
 * 注意：目前，{@link WindowJoinOperator} 不支持早到晚到。 因此，后期元素（元素属于发射窗口）将被简单地删除。
 *
 * <p>Note: currently, {@link WindowJoinOperator} doesn't support DELETE or UPDATE_BEFORE input row.
 * 注意：目前，{@link WindowJoinOperator} 不支持 DELETE 或 UPDATE_BEFORE 输入行。
 */
public abstract class WindowJoinOperator extends TableStreamOperator<RowData>
        implements TwoInputStreamOperator<RowData, RowData, RowData>,
                Triggerable<RowData, Long>,
                KeyContext {

    private static final long serialVersionUID = 1L;

    private static final String LEFT_LATE_ELEMENTS_DROPPED_METRIC_NAME =
            "leftNumLateRecordsDropped";
    private static final String LEFT_LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME =
            "leftLateRecordsDroppedRate";
    private static final String RIGHT_LATE_ELEMENTS_DROPPED_METRIC_NAME =
            "rightNumLateRecordsDropped";
    private static final String RIGHT_LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME =
            "rightLateRecordsDroppedRate";
    private static final String WATERMARK_LATENCY_METRIC_NAME = "watermarkLatency";
    private static final String LEFT_RECORDS_STATE_NAME = "left-records";
    private static final String RIGHT_RECORDS_STATE_NAME = "right-records";

    protected final RowDataSerializer leftSerializer;
    protected final RowDataSerializer rightSerializer;
    private final GeneratedJoinCondition generatedJoinCondition;

    private final int leftWindowEndIndex;
    private final int rightWindowEndIndex;

    private final boolean[] filterNullKeys;
    private final ZoneId shiftTimeZone;

    private transient WindowTimerService<Long> windowTimerService;

    // ------------------------------------------------------------------------
    protected transient JoinConditionWithNullFilters joinCondition;

    /** This is used for emitting elements with a given timestamp. */
    protected transient TimestampedCollector<RowData> collector;

    private transient WindowListState<Long> leftWindowState;
    private transient WindowListState<Long> rightWindowState;

    // ------------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------------

    private transient Counter leftNumLateRecordsDropped;
    private transient Meter leftLateRecordsDroppedRate;
    private transient Counter rightNumLateRecordsDropped;
    private transient Meter rightLateRecordsDroppedRate;
    private transient Gauge<Long> watermarkLatency;

    WindowJoinOperator(
            TypeSerializer<RowData> leftSerializer,
            TypeSerializer<RowData> rightSerializer,
            GeneratedJoinCondition generatedJoinCondition,
            int leftWindowEndIndex,
            int rightWindowEndIndex,
            boolean[] filterNullKeys,
            ZoneId shiftTimeZone) {
        this.leftSerializer = (RowDataSerializer) leftSerializer;
        this.rightSerializer = (RowDataSerializer) rightSerializer;
        this.generatedJoinCondition = generatedJoinCondition;
        this.leftWindowEndIndex = leftWindowEndIndex;
        this.rightWindowEndIndex = rightWindowEndIndex;
        this.filterNullKeys = filterNullKeys;
        this.shiftTimeZone = shiftTimeZone;
    }

    @Override
    public void open() throws Exception {
        super.open();

        this.collector = new TimestampedCollector<>(output);
        collector.eraseTimestamp();

        final LongSerializer windowSerializer = LongSerializer.INSTANCE;

        InternalTimerService<Long> internalTimerService =
                getInternalTimerService("window-timers", windowSerializer, this);
        this.windowTimerService = new WindowTimerServiceImpl(internalTimerService, shiftTimeZone);

        // init join condition
        //初始化join condition
        JoinCondition condition =
                generatedJoinCondition.newInstance(getRuntimeContext().getUserCodeClassLoader());
        this.joinCondition = new JoinConditionWithNullFilters(condition, filterNullKeys, this);
        this.joinCondition.setRuntimeContext(getRuntimeContext());
        this.joinCondition.open(new Configuration());

        // init state
        //初始化状态
        ListStateDescriptor<RowData> leftRecordStateDesc =
                new ListStateDescriptor<>(LEFT_RECORDS_STATE_NAME, leftSerializer);
        ListState<RowData> leftListState =
                getOrCreateKeyedState(windowSerializer, leftRecordStateDesc);
        this.leftWindowState =
                new WindowListState<>((InternalListState<RowData, Long, RowData>) leftListState);

        ListStateDescriptor<RowData> rightRecordStateDesc =
                new ListStateDescriptor<>(RIGHT_RECORDS_STATE_NAME, rightSerializer);
        ListState<RowData> rightListState =
                getOrCreateKeyedState(windowSerializer, rightRecordStateDesc);
        this.rightWindowState =
                new WindowListState<>((InternalListState<RowData, Long, RowData>) rightListState);

        // metrics
        this.leftNumLateRecordsDropped = metrics.counter(LEFT_LATE_ELEMENTS_DROPPED_METRIC_NAME);
        this.leftLateRecordsDroppedRate =
                metrics.meter(
                        LEFT_LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME,
                        new MeterView(leftNumLateRecordsDropped));
        this.rightNumLateRecordsDropped = metrics.counter(RIGHT_LATE_ELEMENTS_DROPPED_METRIC_NAME);
        this.rightLateRecordsDroppedRate =
                metrics.meter(
                        RIGHT_LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME,
                        new MeterView(rightNumLateRecordsDropped));
        this.watermarkLatency =
                metrics.gauge(
                        WATERMARK_LATENCY_METRIC_NAME,
                        () -> {
                            long watermark = windowTimerService.currentWatermark();
                            if (watermark < 0) {
                                return 0L;
                            } else {
                                return windowTimerService.currentProcessingTime() - watermark;
                            }
                        });
    }

    @Override
    public void close() throws Exception {
        super.close();
        collector = null;
        if (joinCondition != null) {
            joinCondition.close();
        }
    }

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        processElement(element, leftWindowEndIndex, leftLateRecordsDroppedRate, leftWindowState);
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        processElement(element, rightWindowEndIndex, rightLateRecordsDroppedRate, rightWindowState);
    }

    private void processElement(
            StreamRecord<RowData> element,
            int windowEndIndex,
            Meter lateRecordsDroppedRate,
            WindowListState<Long> recordState)
            throws Exception {
        //获取输入的数据
        RowData inputRow = element.getValue();
        //获取窗口的结束时间
        long windowEnd = inputRow.getLong(windowEndIndex);
        //判断窗口是否应在当前进度上触发。就是窗口是否已经触发了
        if (isWindowFired(windowEnd, windowTimerService.currentWatermark(), shiftTimeZone)) {
            // element is late and should be dropped
            //因为窗口已经触发所以数据迟到了应该被删除
            lateRecordsDroppedRate.markEvent();
            return;
        }
        //输入的数据类型不是回撤的数据类型
        if (RowDataUtil.isAccumulateMsg(inputRow)) {
            //将数据加入到state中，注意这里使用windowEnd来区分输入的数据
            recordState.add(windowEnd, inputRow);
        } else {
            // Window join could not handle retraction input stream
            //window join不能够处理retraction输入流
            throw new UnsupportedOperationException(
                    "This is a bug and should not happen. Please file an issue.");
        }
        // always register time for every element
        //为每一条数据注册定时器，注册的时间就是数据所在窗口的结束时间，
        //也就是说这个定时器在窗口结束的时候会触发
        windowTimerService.registerEventTimeWindowTimer(windowEnd);
    }

    @Override
    public void onProcessingTime(InternalTimer<RowData, Long> timer) throws Exception {
        // Window join only support event-time now
        //窗口join目前只支持事件时间
        throw new UnsupportedOperationException(
                "This is a bug and should not happen. Please file an issue.");
    }

    @Override
    public void onEventTime(InternalTimer<RowData, Long> timer) throws Exception {
        //设置当前key
        setCurrentKey(timer.getKey());
        //获取window的Namespace，这里就是windowEnd，用它来区分对应的窗口
        Long window = timer.getNamespace();
        // join left records and right records
        //从state获取join左右侧的数据，这里就是在窗口结束的时候获取窗口中的全部内容
        List<RowData> leftData = leftWindowState.get(window);
        List<RowData> rightData = rightWindowState.get(window);
        //join左右窗口中的数据
        join(leftData, rightData);
        // clear state
        //清理左右侧的state中的数据
        if (leftData != null) {
            leftWindowState.clear(window);
        }
        if (rightData != null) {
            rightWindowState.clear(window);
        }
    }

    public abstract void join(Iterable<RowData> leftRecords, Iterable<RowData> rightRecords);

    static class SemiAntiJoinOperator extends WindowJoinOperator {

        private final boolean isAntiJoin;

        SemiAntiJoinOperator(
                TypeSerializer<RowData> leftSerializer,
                TypeSerializer<RowData> rightSerializer,
                GeneratedJoinCondition generatedJoinCondition,
                int leftWindowEndIndex,
                int rightWindowEndIndex,
                boolean[] filterNullKeys,
                boolean isAntiJoin,
                ZoneId shiftTimeZone) {
            super(
                    leftSerializer,
                    rightSerializer,
                    generatedJoinCondition,
                    leftWindowEndIndex,
                    rightWindowEndIndex,
                    filterNullKeys,
                    shiftTimeZone);
            this.isAntiJoin = isAntiJoin;
        }

        @Override
        public void join(Iterable<RowData> leftRecords, Iterable<RowData> rightRecords) {
            if (leftRecords == null) {
                return;
            }
            if (rightRecords == null) {
                if (isAntiJoin) {
                    for (RowData leftRecord : leftRecords) {
                        collector.collect(leftRecord);
                    }
                }
                return;
            }
            for (RowData leftRecord : leftRecords) {
                boolean matches = false;
                for (RowData rightRecord : rightRecords) {
                    if (joinCondition.apply(leftRecord, rightRecord)) {
                        if (joinCondition.apply(leftRecord, rightRecord)) {
                            matches = true;
                            break;
                        }
                    }
                }
                if (matches) {
                    if (!isAntiJoin) {
                        // emit left record if there are matched rows on the other side
                        collector.collect(leftRecord);
                    }
                } else {
                    if (isAntiJoin) {
                        // emit left record if there is no matched row on the other side
                        collector.collect(leftRecord);
                    }
                }
            }
        }
    }

    static class InnerJoinOperator extends WindowJoinOperator {
        private transient JoinedRowData outRow;

        InnerJoinOperator(
                TypeSerializer<RowData> leftSerializer,
                TypeSerializer<RowData> rightSerializer,
                GeneratedJoinCondition generatedJoinCondition,
                int leftWindowEndIndex,
                int rightWindowEndIndex,
                boolean[] filterNullKeys,
                ZoneId shiftTimeZone) {
            super(
                    leftSerializer,
                    rightSerializer,
                    generatedJoinCondition,
                    leftWindowEndIndex,
                    rightWindowEndIndex,
                    filterNullKeys,
                    shiftTimeZone);
        }

        @Override
        public void open() throws Exception {
            super.open();
            outRow = new JoinedRowData();
        }

        @Override
        public void join(Iterable<RowData> leftRecords, Iterable<RowData> rightRecords) {
            //如果某一侧的数据为空直接返回
            if (leftRecords == null || rightRecords == null) {
                return;
            }
            //循环遍历左右侧数据进行join
            for (RowData leftRecord : leftRecords) {
                for (RowData rightRecord : rightRecords) {
                    //将数据带入如果满足join condition就返回数据
                    if (joinCondition.apply(leftRecord, rightRecord)) {
                        outRow.setRowKind(RowKind.INSERT);
                        outRow.replace(leftRecord, rightRecord);
                        collector.collect(outRow);
                    }
                }
            }
        }
    }

    private abstract static class AbstractOuterJoinOperator extends WindowJoinOperator {

        private static final long serialVersionUID = 1L;

        private transient RowData leftNullRow;
        private transient RowData rightNullRow;
        private transient JoinedRowData outRow;

        AbstractOuterJoinOperator(
                TypeSerializer<RowData> leftSerializer,
                TypeSerializer<RowData> rightSerializer,
                GeneratedJoinCondition generatedJoinCondition,
                int leftWindowEndIndex,
                int rightWindowEndIndex,
                boolean[] filterNullKeys,
                ZoneId shiftTimeZone) {
            super(
                    leftSerializer,
                    rightSerializer,
                    generatedJoinCondition,
                    leftWindowEndIndex,
                    rightWindowEndIndex,
                    filterNullKeys,
                    shiftTimeZone);
        }

        @Override
        public void open() throws Exception {
            super.open();
            leftNullRow = new GenericRowData(leftSerializer.getArity());
            rightNullRow = new GenericRowData(rightSerializer.getArity());
            outRow = new JoinedRowData();
        }

        protected void outputNullPadding(RowData row, boolean isLeft) {
            if (isLeft) {
                outRow.replace(row, rightNullRow);
            } else {
                outRow.replace(leftNullRow, row);
            }
            outRow.setRowKind(RowKind.INSERT);
            collector.collect(outRow);
        }

        protected void outputNullPadding(Iterable<RowData> rows, boolean isLeft) {
            for (RowData row : rows) {
                outputNullPadding(row, isLeft);
            }
        }

        protected void output(RowData inputRow, RowData otherRow, boolean inputIsLeft) {
            if (inputIsLeft) {
                outRow.replace(inputRow, otherRow);
            } else {
                outRow.replace(otherRow, inputRow);
            }
            outRow.setRowKind(RowKind.INSERT);
            collector.collect(outRow);
        }
    }

    static class LeftOuterJoinOperator extends AbstractOuterJoinOperator {

        private static final long serialVersionUID = 1L;

        LeftOuterJoinOperator(
                TypeSerializer<RowData> leftSerializer,
                TypeSerializer<RowData> rightSerializer,
                GeneratedJoinCondition generatedJoinCondition,
                int leftWindowEndIndex,
                int rightWindowEndIndex,
                boolean[] filterNullKeys,
                ZoneId shiftTimeZone) {
            super(
                    leftSerializer,
                    rightSerializer,
                    generatedJoinCondition,
                    leftWindowEndIndex,
                    rightWindowEndIndex,
                    filterNullKeys,
                    shiftTimeZone);
        }

        @Override
        public void join(Iterable<RowData> leftRecords, Iterable<RowData> rightRecords) {
            if (leftRecords == null) {
                return;
            }
            if (rightRecords == null) {
                outputNullPadding(leftRecords, true);
            } else {
                for (RowData leftRecord : leftRecords) {
                    boolean matches = false;
                    for (RowData rightRecord : rightRecords) {
                        if (joinCondition.apply(leftRecord, rightRecord)) {
                            output(leftRecord, rightRecord, true);
                            matches = true;
                        }
                    }
                    if (!matches) {
                        // padding null for left side
                        outputNullPadding(leftRecord, true);
                    }
                }
            }
        }
    }

    static class RightOuterJoinOperator extends AbstractOuterJoinOperator {

        private static final long serialVersionUID = 1L;

        RightOuterJoinOperator(
                TypeSerializer<RowData> leftSerializer,
                TypeSerializer<RowData> rightSerializer,
                GeneratedJoinCondition generatedJoinCondition,
                int leftWindowEndIndex,
                int rightWindowEndIndex,
                boolean[] filterNullKeys,
                ZoneId shiftTimeZone) {
            super(
                    leftSerializer,
                    rightSerializer,
                    generatedJoinCondition,
                    leftWindowEndIndex,
                    rightWindowEndIndex,
                    filterNullKeys,
                    shiftTimeZone);
        }

        @Override
        public void join(Iterable<RowData> leftRecords, Iterable<RowData> rightRecords) {
            if (rightRecords == null) {
                return;
            }
            if (leftRecords == null) {
                outputNullPadding(rightRecords, false);
            } else {
                for (RowData rightRecord : rightRecords) {
                    boolean matches = false;
                    for (RowData leftRecord : leftRecords) {
                        if (joinCondition.apply(leftRecord, rightRecord)) {
                            output(leftRecord, rightRecord, true);
                            matches = true;
                        }
                    }
                    if (!matches) {
                        outputNullPadding(rightRecord, false);
                    }
                }
            }
        }
    }

    static class FullOuterJoinOperator extends AbstractOuterJoinOperator {

        private static final long serialVersionUID = 1L;

        FullOuterJoinOperator(
                TypeSerializer<RowData> leftSerializer,
                TypeSerializer<RowData> rightSerializer,
                GeneratedJoinCondition generatedJoinCondition,
                int leftWindowEndIndex,
                int rightWindowEndIndex,
                boolean[] filterNullKeys,
                ZoneId shiftTimeZone) {
            super(
                    leftSerializer,
                    rightSerializer,
                    generatedJoinCondition,
                    leftWindowEndIndex,
                    rightWindowEndIndex,
                    filterNullKeys,
                    shiftTimeZone);
        }

        @Override
        public void join(Iterable<RowData> leftRecords, Iterable<RowData> rightRecords) {
            if (leftRecords == null && rightRecords == null) {
                return;
            }
            if (rightRecords == null) {
                outputNullPadding(leftRecords, true);
            } else if (leftRecords == null) {
                outputNullPadding(rightRecords, false);
            } else {
                IdentityHashMap<RowData, Boolean> emittedRightRecords = new IdentityHashMap<>();
                for (RowData leftRecord : leftRecords) {
                    boolean matches = false;
                    for (RowData rightRecord : rightRecords) {
                        if (joinCondition.apply(leftRecord, rightRecord)) {
                            output(leftRecord, rightRecord, true);
                            matches = true;
                            emittedRightRecords.put(rightRecord, Boolean.TRUE);
                        }
                    }
                    // padding null for left side
                    if (!matches) {
                        outputNullPadding(leftRecord, true);
                    }
                }
                // padding null for never emitted right side
                for (RowData rightRecord : rightRecords) {
                    if (!emittedRightRecords.containsKey(rightRecord)) {
                        outputNullPadding(rightRecord, false);
                    }
                }
            }
        }
    }
}
