package com.dlb.etl

import com.dlb.common.toJson
import org.apache.hadoop.hbase.HBaseConfiguration
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


object KafkaConsume {

  def main(args: Array[String]): Unit = {

      implicit val conf: Config = ConfigFactory.load

    val sparkConf = new SparkConf().setAppName("streaming").setMaster("local[*]").set("spark.executor.memory","4g")
    val sparkContext = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkContext,Seconds(60))

    val topic = "test2"
    val topics = Array(topic)
    val tableName = "history_cibn"

    val kafkaParams = mutable.HashMap[String,String]()
        kafkaParams.put("bootstrap.servers" ,"hadoop01:9092,hadoop02:9092,hadoop03:9092")
        kafkaParams.put("group.id", "kafkagp")
        kafkaParams.put("auto.offset.reset","latest")
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        kafkaParams.put("value.deserializer" , "org.apache.kafka.common.serialization.StringDeserializer")

    val stream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

    val jobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val input = stream.flatMap(line => {
      Some(line.value.toString)
    })

     val lines = stream.map(_.value())

      lines.foreachRDD(rdd =>{
          if (!rdd.isEmpty()){
              rdd.saveAsTextFile("hdfs://hadoop01:9000/jsonDatas/"+System.currentTimeMillis())
          }
      })

        input.foreachRDD(rdd => {
          if (!rdd.isEmpty()) {
            val spark1 = SparkSession.builder.config(sparkContext.getConf).getOrCreate
            val df = spark1.read.json(rdd)
            //        implicit val matchError = org.apache.spark.sql.Encoders.kryo[Row]
            df.createOrReplaceTempView("tempTable")

            import spark1.implicits._
            val ans = spark1.sql("select analyticId,androidId,bdCpu,bdModel,buildBoard,channel,cid,city,cityCode,country,cpuCnt,cpuName," +
              "definition,deviceId,deviceName,dpi,duration,endTime,entry1,entry1Id,eth0Mac,eventKey,eventName,eventType,eventValue,ip," +
              "ipaddr,isVip,isp,kafkaTopic,largeMem,limitMem,name,nameId,openId,optType,page,pkg,pluginPkg,pluginVercode,pos,prePage," +
              "prevue,province,rectime,screen,serial,session,site,specId,subName,time,topic,topicCid,topicId,topicType,touch,uid,url," +
              "uuid,verCode,verName,wlan0Mac,x,y from tempTable").as[toJson].rdd

                    ans.map(item =>{
                      val put = new Put(Bytes.toBytes(item.uuid+"_"+item.androidId))
    //                  put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("adType"),Bytes.toBytes(item.adType))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("analyticId"),Bytes.toBytes(item.analyticId))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("androidId"),Bytes.toBytes(item.androidId))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("bdCpu"),Bytes.toBytes(item.bdCpu))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("bdModel"),Bytes.toBytes(item.bdModel))
    //                  put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("blockId"),Bytes.toBytes(item.blockId))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("buildBoard"),Bytes.toBytes(item.buildBoard))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("channel"),Bytes.toBytes(item.channel))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("cid"),Bytes.toBytes(item.cid))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("city"),Bytes.toBytes(item.city))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("cityCode"),Bytes.toBytes(item.cityCode))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("country"),Bytes.toBytes(item.country))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("cpuCnt"),Bytes.toBytes(item.cpuCnt))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("cpuName"),Bytes.toBytes(item.cpuName))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("definition"),Bytes.toBytes(item.definition))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("deviceId"),Bytes.toBytes(item.deviceId))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("deviceName"),Bytes.toBytes(item.deviceName))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("dpi"),Bytes.toBytes(item.dpi))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("duration"),Bytes.toBytes(item.duration))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("endTime"),Bytes.toBytes(item.endTime))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("entry1"),Bytes.toBytes(item.entry1))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("entry1Id"),Bytes.toBytes(item.entry1Id))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("eth0Mac"),Bytes.toBytes(item.eth0Mac))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("eventKey"),Bytes.toBytes(item.eventKey))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("eventName"),Bytes.toBytes(item.eventName))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("eventType"),Bytes.toBytes(item.eventType))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("eventValue"),Bytes.toBytes(item.eventValue))
    //                  put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("index"),Bytes.toBytes(item.index))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("ip"),Bytes.toBytes(item.ip))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("ipaddr"),Bytes.toBytes(item.ipaddr))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("isVip"),Bytes.toBytes(item.isVip))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("isp"),Bytes.toBytes(item.isp))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("kafkaTopic"),Bytes.toBytes(item.kafkaTopic))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("largeMem"),Bytes.toBytes(item.largeMem))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("limitMem"),Bytes.toBytes(item.limitMem))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("name"),Bytes.toBytes(item.name))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("nameId"),Bytes.toBytes(item.nameId))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("openId"),Bytes.toBytes(item.openId))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("optType"),Bytes.toBytes(item.optType))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("page"),Bytes.toBytes(item.page))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("pkg"),Bytes.toBytes(item.pkg))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("pluginPkg"),Bytes.toBytes(item.pluginPkg))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("pluginVercode"),Bytes.toBytes(item.pluginVercode))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("pos"),Bytes.toBytes(item.pos))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("prePage"),Bytes.toBytes(item.prePage))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("prevue"),Bytes.toBytes(item.prevue))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("province"),Bytes.toBytes(item.province))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("rectime"),Bytes.toBytes(item.rectime))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("screen"),Bytes.toBytes(item.screen))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("serial"),Bytes.toBytes(item.serial))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("session"),Bytes.toBytes(item.session))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("site"),Bytes.toBytes(item.site))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("specId"),Bytes.toBytes(item.specId))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("subName"),Bytes.toBytes(item.subName))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("time"),Bytes.toBytes(item.time))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("topic"),Bytes.toBytes(item.topic))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("topicCid"),Bytes.toBytes(item.topicCid))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("topicId"),Bytes.toBytes(item.topicId))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("topicType"),Bytes.toBytes(item.topicType))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("touch"),Bytes.toBytes(item.touch))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("uid"),Bytes.toBytes(item.uid))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("url"),Bytes.toBytes(item.url))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("uuid"),Bytes.toBytes(item.uuid))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("verCode"),Bytes.toBytes(item.verCode))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("verName"),Bytes.toBytes(item.verName))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("wlan0Mac"),Bytes.toBytes(item.wlan0Mac))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("x"),Bytes.toBytes(item.x))
                      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("y"),Bytes.toBytes(item.y))
                      (new ImmutableBytesWritable,put)
                    }).saveAsHadoopDataset(jobConf)
          }
        })

    ssc.start()
    ssc.awaitTermination()
  }
}
