package com.sixth.flink.stream

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 14:55 2018/10/8
  * @ 
  */
object ScalaStreamingWordCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dStream: DataStream[String] = env.socketTextStream("hadoop05", 6666)

    val value: KeyedStream[(String, Int), Tuple] = dStream.flatMap(_.split(" ")).map((_, 1)).keyBy(0)

    val sumed: DataStream[(String, Int)] = value.timeWindow(Time.seconds(5)).sum(1)

    sumed.print()

    env.execute()
  }
}
