package com.sixth.flink.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 14:38 2018/10/8
 * @
 */
public class JavaStreamingWordCount {
    public static void main(String[] args) throws Exception {
        // 初始化，注意是Java的包
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取数据，监听netcat服务，5秒获取一次数据并处理
        DataStream<String> data = env.socketTextStream("hadoo05", 6666);

        // 处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> pairs = data.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for (String word : value.split(" ")) {
                            out.collect(new Tuple2<String, Integer>(word, 1));
                        }
                    }
                });

        // 指定以那个字段作为key
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByPairs = pairs.keyBy(0);

        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> batchData =
                keyByPairs.timeWindow(Time.seconds(5));

        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = batchData.sum(1);

        summed.print();

        // 开启Streaming程序
        env.execute("Java Streaming Word Count");

    }
}
