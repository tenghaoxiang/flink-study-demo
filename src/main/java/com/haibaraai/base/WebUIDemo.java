package com.haibaraai.base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author tenghaoxiang
 * @Date: 2021/8/18 11:14 下午
 */
public class WebUIDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStream<String> stringDataStream = env.socketTextStream("127.0.0.1", 8888);
        DataStream<String> flatMapStream = stringDataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> collector) throws Exception {
                String[] strings = value.split(",");
                for (String string : strings) {
                    collector.collect(string);
                }
            }
        });
        flatMapStream.print("result");
        env.execute("flink web ui demo");
    }
}
