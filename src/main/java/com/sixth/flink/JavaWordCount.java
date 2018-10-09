package com.sixth.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 10:55 2018/10/8
 * @
 */
public class JavaWordCount {
    public static void main(String[] args) throws Exception {
        /*
         * 模板代码
         * */
        // 构建flink环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 通过字符串来构建数据集
        DataSource<String> data = env.fromElements("hello xiaoli", "hello xiaoxiao", "hello xiaofang");

        // 处理数据，切分字符串并按照key进行分组，统计相同key的个数
        AggregateOperator<Tuple2<String, Integer>> res = data.flatMap(new SpliterData())
                .groupBy(0) // 按照Tuple2的第一个进行分组
                .sum(1); // 按照Tuple2的第二个进行sum

        res.print();
    }

    /**
     * 内部类，用于切分数据
     */
    private static class SpliterData implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            for (String word : line.split(" ")) {
                // 不用写return，直接使用out输出就可以了
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
