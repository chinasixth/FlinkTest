package com.sixth.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 14:19 2018/10/8
 * @
 */
public class FlinkDataSourceAndSink {
    public static void main(String[] args) throws Exception {
        // 初始化
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 指定元素创建DataSet
        DataSet<String> data = env.fromElements("Java", "Python", "Scala", "Shell", "JavaScript");
        data.print();

//        // 指定元素生成Sequence
//        DataSet<Long> numbers = env.generateSequence(1, 10);
//        numbers.print(); // 结果是无序的

        // 从本地读取数据
        DataSet<String> localLines = env.readTextFile("E:\\ideaJavaProject\\FlinkTest\\src\\main\\java\\com\\sixth\\data\\test.txt");
        localLines.print();

//        // 从HDFS读取数据
//        DataSet<String> hdfsLines = env.readTextFile("hdfs://hadoop05:9000/wc");

//        // 读取Linux上的文件
//        DataSet<String> linuxLines = env.readTextFile("file://home/test.txt");

//        // 输出到本地
//        data.writeAsText("E:\\ideaJavaProject\\FlinkTest\\src\\main\\java\\com\\sixth\\data\\out.txt");

//        // 输出到HDFS，第二个参数表示是否覆盖
//        data.writeAsText("hdfs://hadoop05:9000/out.txt", FileSystem.WriteMode.OVERWRITE);

        // action类型的算子触发一下
        env.execute();
    }
}
