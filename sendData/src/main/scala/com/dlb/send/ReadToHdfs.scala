package com.dlb.send

import com.dlb.commons.sendPara
import com.google.gson.Gson
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.http.{Header, HttpEntity}
import org.apache.spark.{SparkConf, SparkContext}

object ReadToHdfs {

  def main(args: Array[String]): Unit = {

    System.setProperty("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val conf = new SparkConf().setAppName("readJson").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val fileRdd = sc.textFile("hdfs://hadoop01:9000/jsonDatas/*")
    val LineCount = fileRdd.count().toString

    val spock = new sendPara("R5dKCJdf5iuXd1g1xvEDDJ4fgw319L5NA564UzZJf6wGXqzg5CriocZqQstndJB4VhhJcASAembz9ygmsKikpYfcT6CWmHFmbLZ2",
                              "69056d74febb7f15c33f1b44aca343799065ad8c4df505058f6a7c4866495f37",
                              "559faffb1f011d87301e7c92efa748b51bb03298095384070edb077059eed516","101",LineCount)
    val spockAsJson = new Gson().toJson(spock)
    val client = HttpClients.createDefault()
    val post: HttpPost = new HttpPost("http://5qjmi5.natappfree.cc/api/data/uploadData")
    post.addHeader("Content-Type", "application/json")

    post.setEntity(new StringEntity(spockAsJson))
    val response: CloseableHttpResponse = client.execute(post)
    println(spockAsJson)

    val allHeaders: Array[Header] = post.getAllHeaders
    val entity: HttpEntity = response.getEntity
    val string = EntityUtils.toString(entity, "UTF-8")

    println(allHeaders)
    println(string)
    println(entity)


    sc.stop()

  }

}
