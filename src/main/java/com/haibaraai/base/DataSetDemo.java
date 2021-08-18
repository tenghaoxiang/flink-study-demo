package com.haibaraai.base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author tenghaoxiang
 * @Date: 2021/8/18 11:59 下午
 */
public class DataSetDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataSet<String> stringDataSet = env.fromElements("java,springboot", "java,springcloud", "java,servicecomb");
        stringDataSet.print("before process");
        DataSet<String> flatMapSet = stringDataSet.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> collector) throws Exception {
                String[] strings = value.split(",");
                for (String string : strings) {
                    collector.collect(string);
                }
            }
        });
        flatMapSet.print("after process");
        env.execute();
    }
}
