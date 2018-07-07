package org.timchen.dslab5

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.mongodb.spark.MongoSpark
import org.apache.spark.SparkConf
import org.bson.Document
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.zookeeper._
import org.apache.zookeeper.data.Stat
import Thread.sleep

import scala.math.BigDecimal.RoundingMode

object main {
  val mongoURI = "mongodb://mongos1/test.test"
  val brokers = "node1:9094,node2:9094,node3:9094"
  val topics = "tx"
  val zknode = "zoo1:2181,zoo2:2181,zoo3:2181"
  val watcher = new Watcher {
    override def process(watchedEvent: WatchedEvent): Unit =
      System.out.println(watchedEvent.toString)
  }
  val zk = new ZooKeeper(zknode, 5000, watcher)

  def ExchangeRate(i: String, r: String, time: Date): BigDecimal = {
    val start = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss").parse("2018-1-1 00:00:00")
    val diff = (time.getTime - start.getTime) / 60000
    while (zk.exists("/rate_table/" + diff.toString, true) == null) {
      //sleep(1000)
    }
    while (true) {
      val jString = new String(zk.getData("/rate_table/" + diff.toString, true, new Stat()))
      val jObject = JSON.parseObject(jString)
      val irate = BigDecimal(jObject.getBigDecimal(i))
      val rrate = BigDecimal(jObject.getBigDecimal(r))
      if (irate != BigDecimal(-1) && rrate != BigDecimal(-1))
        return irate / rrate
    }
    return -1
  }

  def RoundToMinute(t: Date): Date = new Date(t.getTime / 60000 * 60000)

  def main(args: Array[String]){
    val conf = new SparkConf().setMaster("yarn-cluster")
      .setAppName("dslab5")
      .set("spark.mongodb.output.uri", mongoURI)
    val sc = new StreamingContext(conf, Durations.seconds(2))
    val messages = KafkaUtils.createStream(sc, zknode, "spark-streaming-consumer", Map(topics -> 1)).map(_._2)
    val orders = messages.map(x => new Order(x))
    val inList = orders.map(x => (new Key(x.initiator, RoundToMinute(x.time)), (x.turnover, BigDecimal(0))))
    val outList = orders.map(x => (new Key(x.recipient, RoundToMinute(x.time)),
      (BigDecimal(0), (x.turnover * ExchangeRate(x.initiator, x.recipient, x.time)).setScale(2, RoundingMode.HALF_UP))))
    val result = inList.union(outList)
        .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
        .map(x => new Result(x._1.name, x._2._2, x._2._1, x._1.time))
        .map(_.toString)
        .map(x => Document.parse(x))
    result.foreachRDD(rdd => {
      System.err.println("write db")
      MongoSpark.save(rdd)
    })
    sc.start()
    sc.awaitTermination()
  }
}
