package com.sixth.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 11:42 2018/10/8
 * @
 */
public class TransformationAPIDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

//        // map操作
//        DataSource<Tuple2<Integer, Integer>> intPairs = env.fromElements(
//                new Tuple2<Integer, Integer>(1, 2),
//                new Tuple2<Integer, Integer>(3, 4));
//        MapOperator<Tuple2<Integer, Integer>, Integer> summed = intPairs.map(
//                new MapFunction<Tuple2<Integer, Integer>, Integer>() {
//                    @Override
//                    public Integer map(Tuple2<Integer, Integer> tup) throws Exception {
//                        return tup.f0 + tup.f1;
//                    }
//                });
//
//        summed.print();

//        // flatMap操作
//
//        DataSet<String> lines = env.fromElements("hello xiaofen", "hello xiaoli", "hello xiaofang");
//
//        FlatMapOperator<String, String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public void flatMap(String s, Collector<String> out) throws Exception {
//                for (String word : s.split(" ")) {
//                    out.collect(word);
//                }
//            }
//        });
//
//        words.print();

//        // mapPartition练习 需求：统计每个分区的单词的个数
//        MapPartitionOperator<String, Long> summed = lines.mapPartition(
//                new MapPartitionFunction<String, Long>() {
//                    @Override
//                    public void mapPartition(Iterable<String> value, Collector<Long> out) throws Exception {
//                        Iterator<String> it = value.iterator();
//                        long num = 0;
//                        while (it.hasNext()) {
//                            String str = it.next();
//                            String[] splitted = str.split(" ");
//                            num += splitted.length;
//                        }
//                        out.collect(num);
//                    }
//                });
//
//        summed.print();

//        // filter练习
//        DataSource<Integer> nums = env.fromElements(1, 2, 3, 4, 5, 6, 7);
//        FilterOperator<Integer> filtered = nums.filter(new FilterFunction<Integer>() {
//            @Override
//            public boolean filter(Integer integer) throws Exception {
//                return integer > 4;
//            }
//        });
//
//        filtered.print();
    }
}
