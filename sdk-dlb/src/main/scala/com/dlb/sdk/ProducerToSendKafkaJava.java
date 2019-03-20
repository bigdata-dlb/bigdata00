package com.dlb.sdk;

import com.google.gson.Gson;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.JaasUtils;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ProducerToSendKafkaJava {

    public static void main(String[] args){
        String url = "http://192.168.1.160:8080/app/user/login.xhtml";
        String requestJson="";
//        String resultJson=HttpClientUtil.postFromJson(url,requestJson);
        String resultJson="{\n" +
                "    \"appkey\": true,\n" +
                "    \"topic\": \"test2\"\n" +
                "}";
        Map resultMap=new Gson().fromJson(resultJson,Map.class);
        boolean appkey= (boolean) resultMap.get("appkey");
        String topic=resultMap.get("topic")+"";
        String brokerList = "192.168.10.90:9092,192.168.10.92:9092";

        Properties properties = new Properties();
        properties.put("bootstrap.servers",brokerList);
        properties.put("acks", "1");
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = null;

        if(appkey){
            ZkUtils zkUtils = ZkUtils.apply("192.168.10.90:2181,192.168.10.231:2181,192.168.10.92:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());

            if (AdminUtils.topicExists(zkUtils,topic) == false){
                AdminUtils.createTopic(zkUtils, topic, 6, 1, new Properties(), RackAwareMode.Enforced$.MODULE$);
                zkUtils.close();
            }else {
                // Scan
                String path = "H:\\Cango\\testSource\\One";
                File file=new File(path);
                String[] strings=new String[2];
                List list= (List) org.apache.commons.io.FileUtils.listFiles(file,null,true);

                for (int i =0; i<list.size();i++){
                    try {
                        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(new File(list.get(i).toString())));
                        BufferedReader reader = new BufferedReader(new InputStreamReader(bis, "utf-8"), 10 * 1024 * 1024);// 10M cache
                        String line = "";
                        producer = new KafkaProducer(properties);
                        while((line = reader.readLine()) != null){
                            //Send msg......
                            ProducerRecord<String,String> message = new ProducerRecord<String, String>(topic,line);
                            producer.send(message);
                            System.out.println(message);
                        }
                    }catch (IOException e){
                        e.printStackTrace();
                    }
                }
            }
        }else {
            System.out.println("........！");
        }
    }
}
