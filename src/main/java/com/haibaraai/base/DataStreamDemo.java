package com.haibaraai.base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author tenghaoxiang
 * @Date: 2021/8/18 11:52 下午
 */
public class DataStreamDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> stringDataStream = env.fromElements("java,springboot", "java,springcloud", "java,servicecomb");
        stringDataStream.print("before process");
        DataStream<String> flatMapStream = stringDataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> collector) throws Exception {
                String[] strings = value.split(",");
                for (String string : strings) {
                    collector.collect(string);
                }
            }
        });
        flatMapStream.print("after process");
        env.execute();
    }
}
