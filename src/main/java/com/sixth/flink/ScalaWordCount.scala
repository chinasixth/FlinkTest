package com.sixth.flink

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 11:15 2018/10/8
  * @ 
  */
object ScalaWordCount {
  /*
  * import org.apache.flink.streaming.api.scala._
  * 如果数据是有限的（静态数据集），我们可以引入以下包：
  * import org.apache.flink.api.scala._
  * */

  def main(args: Array[String]): Unit = {
    // 构建flink环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 离线处理，即数据有限，引入下面的这个隐式转换
    import org.apache.flink.api.scala._

    // 构建数据集
    val data: DataSet[String] = env.fromElements("hello xiaoli", "hello xiaofang", "hello jiege")

    // 处理数据: 切分，分组，聚合
    val res: AggregateDataSet[(String, Int)] = data.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    res.print()

    res.writeAsText("c://haha", FileSystem.WriteMode.OVERWRITE)

    // 开始执行
    env.execute()
  }
}
