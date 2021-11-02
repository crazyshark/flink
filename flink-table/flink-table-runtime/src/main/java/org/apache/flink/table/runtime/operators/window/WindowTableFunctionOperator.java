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

package org.apache.flink.table.runtime.operators.window;

import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.operators.window.assigners.WindowAssigner;

import java.time.ZoneId;
import java.util.Collection;

import static org.apache.flink.table.runtime.util.TimeWindowUtil.toEpochMills;
import static org.apache.flink.table.runtime.util.TimeWindowUtil.toUtcTimestampMills;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * The WindowTableFunctionOperator acts as a table-valued function to assign windows for input row.
 * Output row includes the original columns as well additional 3 columns named {@code window_start},
 * {@code window_end}, {@code window_time} to indicate the assigned window.
 * WindowTableFunctionOperator 充当表值函数，为输入行分配窗口。 输出行包括原始列以及名为 {@code window_start}、
 * {@code window_end}、{@code window_time} 的附加 3 列以指示分配的窗口。
 *
 * <p>Note: The operator only works for row-time.
 * 目前只能用于事件时间
 *
 * <p>Note: The operator emits per record instead of at the end of window.
 * 注意：操作符每条记录发出，而不是在窗口结束时发出。
 */
public class WindowTableFunctionOperator extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private final WindowAssigner<TimeWindow> windowAssigner;
    private final int rowtimeIndex;

    /**
     * The shift timezone of the window, if the proctime or rowtime type is TIMESTAMP_LTZ, the shift
     * timezone is the timezone user configured in TableConfig, other cases the timezone is UTC
     * which means never shift when assigning windows.
     * 窗口的移位时区，如果proctime或rowtime类型为TIMESTAMP_LTZ，则移位时区为用户在TableConfig中配置的时区，
     * 其他情况时区为UTC，即分配窗口时永不移位。
     */
    private final ZoneId shiftTimeZone;

    /** This is used for emitting elements with a given timestamp. */
    //用来发送给定时间戳的数据
    private transient TimestampedCollector<RowData> collector;

    private transient JoinedRowData outRow;
    private transient GenericRowData windowProperties;

    public WindowTableFunctionOperator(
            WindowAssigner<TimeWindow> windowAssigner, int rowtimeIndex, ZoneId shiftTimeZone) {
        checkArgument(windowAssigner.isEventTime() && rowtimeIndex >= 0);
        this.windowAssigner = windowAssigner;
        this.rowtimeIndex = rowtimeIndex;
        this.shiftTimeZone = shiftTimeZone;

        setChainingStrategy(ChainingStrategy.ALWAYS);
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.collector = new TimestampedCollector<>(output);
        collector.eraseTimestamp();

        outRow = new JoinedRowData();
        windowProperties = new GenericRowData(3);
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        //获取数据
        RowData inputRow = element.getValue();
        //获取数据的时间戳
        long timestamp = inputRow.getTimestamp(rowtimeIndex, 3).getMillisecond();
        timestamp = toUtcTimestampMills(timestamp, shiftTimeZone);
        //获取数据所属的窗口
        Collection<TimeWindow> elementWindows = windowAssigner.assignWindows(inputRow, timestamp);
        //循环所有窗口，将每个窗口的信息创建windowProperties，然后将数据和windowProperties发送到下游
        for (TimeWindow window : elementWindows) {
            //设置window的start
            windowProperties.setField(0, TimestampData.fromEpochMillis(window.getStart()));
            //设置window的end
            windowProperties.setField(1, TimestampData.fromEpochMillis(window.getEnd()));
            //设置窗口的时区信息？
            windowProperties.setField(
                    2,
                    TimestampData.fromEpochMillis(
                            toEpochMills(window.maxTimestamp(), shiftTimeZone)));
            //将数据和窗口的信息发送到下游
            collector.collect(outRow.replace(inputRow, windowProperties));
        }
    }
}
