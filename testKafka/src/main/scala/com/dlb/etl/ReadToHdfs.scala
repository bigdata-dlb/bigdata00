package com.dlb.etl

import org.apache.spark.{SparkConf, SparkContext}

object ReadToHdfs {

  def main(args: Array[String]): Unit = {

    System.setProperty("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //初始化spark

    val conf = new SparkConf().setAppName("readJson").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val fileRdd = sc.textFile("hdfs://hadoop01:9000/jsonDatas/*")
    println(fileRdd + ",该文件的行数为：" + fileRdd.count())
    println(fileRdd.count())
    sc.stop()

  }

}
