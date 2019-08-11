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

package com.ml.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStateBackend(new FsStateBackend("file:///tmp/lala",false));

        env.enableCheckpointing(20000);

		// execute program
        KeyedStream<TestDto, String> keyed = env.socketTextStream("127.0.0.1", 9998).map(new MapFunction<String, TestDto>() {
            @Override
            public TestDto map(String str) {
                String[] s = str.split(",");
                return new TestDto(s[0], Integer.parseInt(s[1]), new Date(Long.parseLong(s[2])));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TestDto>() {
            @Override
            public long extractAscendingTimestamp(TestDto element) {
                return element.time.getTime();
            }
        }).keyBy(new KeySelector<TestDto, String>() {
            @Override
            public String getKey(TestDto testDto) {
                return testDto.getId();
            }
        });
        WindowedStream<TestDto, String, TimeWindow> window = keyed.window(SlidingEventTimeWindows.of(Time.days(10), Time.days(1)))
                .trigger(new Trigger<TestDto, TimeWindow>() {
                    @Override
                    public TriggerResult onElement(TestDto element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        System.out.printf(window.toString());
                        return TriggerResult.FIRE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return null;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return null;
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

                    }
                });

        window.aggregate(new AVGAgg(), new ProcessWindowFunction<Double, Object, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<Double> elements, Collector<Object> out) {
                System.out.println(s+"="+elements.iterator().next());
            }
        });



        env.execute("test1");

	}
}
