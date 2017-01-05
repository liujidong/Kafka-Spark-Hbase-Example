/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.buildoop.spark.auditactivelogins.job;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import scala.Tuple2;

import java.io.Reader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount. Usage:
 * JavaDirectKafkaWordCount <brokers> <topics> <brokers> is a list of one or
 * more Kafka brokers <topics> is a list of one or more kafka topics to consume
 * from
 *
 * Example: $ bin/run-example streaming.JavaDirectKafkaWordCount
 * broker1-host:port,broker2-host:port \ topic1,topic2
 */

public final class SparkStreamingFromFileToGP {
    public static void main(String[] args) throws Exception {

        String brokers = "64.71.156.203:9092,64.71.156.204:9092,64.71.156.205:9092";
        String topics = "scala_test";

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName("SparkDirectKafkaLog").setMaster("local[2]");//spark://192.168.250.21:7077");//local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        Set<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("group.id","spark_kafka_log_test");

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet);
        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();
        JavaDStream<String> lines = messages.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> rdd) throws Exception {
                OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                offsetRanges.set(offsets);
                return rdd.map(new Function<Tuple2<String, String>, String>() {
                    @Override
                    public String call(Tuple2<String, String> tuple2) {
                        return tuple2._2();
                    }
                });
            }
        });
        final Map<String,String> sessionmap = new HashMap<String, String>();
//        System.out.println("----------------"+sessionmap.size());
        JavaDStream<Map<String,String>> properties = lines.map(new Function<String, Map<String,String>>() {
//            Map<String,String> sessionmap = new HashMap<String, String>();
            @Override
            public Map<String,String> call(String s) throws Exception {
                Map<String,String> map = new HashMap<String, String>();
//                Map<String,String> sessionmap = new HashMap<String, String>();
                try {
                    JSONObject jsonObject = new JSONObject(s);
                    JSONObject properties = (JSONObject)jsonObject.get("properties");
                    String event =(String)jsonObject.get("event");
                    if(event.equals("session")){
                        String uid = properties.getString("uid");
                        String sid = properties.getString("sid");
                        String vendor = properties.getString("vendor");
                        String model = properties.getString("model");
                        String carrier = properties.getString("carrier");
                        String country = properties.getString("country");
                        String site = properties.getString("site");
                        sessionmap.put(uid+"\001"+sid,vendor+'\001'+model+'\001'+carrier+'\001'+country+'\001'+site);
                    }else {
                        Iterator it2 = properties.keys();
                        while (it2.hasNext())
                        {
                            String key = String.valueOf(it2.next());
                            map.put(key,properties.getString(key));
                        }
                        String uid = properties.getString("uid");
                        String sid = properties.getString("sid");
                        if(sessionmap.containsKey(uid+"\001"+sid)){
                            String attrs = sessionmap.get(uid+"\001"+sid);
                            String vendor = attrs.split("\\001")[0];
                            String model = attrs.split("\\001")[1];
                            String carrier = attrs.split("\\001")[2];
                            String country = attrs.split("\\001")[3];
                            String site = attrs.split("\\001")[4];
                            map.put("vendor",vendor);
                            map.put("model",model);
                            map.put("carrier",carrier);
                            map.put("country",country);
                            map.put("site",site);
                        }
                    }
                }catch (JSONException e){
                    e.printStackTrace();
                }
                return map;
            }
        });
        properties.foreachRDD(new Function2<JavaRDD<Map<String, String>>, Time, Void>() {
            @Override
            public Void call(JavaRDD<Map<String, String>> mapJavaRDD, Time time) throws Exception {
                final long count = mapJavaRDD.count();
                System.out.println("================count==================="+count);
                final Date now = new Date();
                mapJavaRDD.foreach(new VoidFunction<Map<String, String>>() {
                    long index = 0;
                    StringBuffer strAll = new StringBuffer();
                    @Override
                    public void call(Map<String, String> map) throws Exception {
                        if(map.size()>0){
                            String rsl = map.get("eventid") + "\001"
                                    +map.get("event")+ "\001"
                                    + map.get("pid") + "\001"
                                    + map.get("sid") + "\001"
                                    + map.get("campaign") + "\001"
                                    + map.get("net") + "\001"
                                    + map.get("uid") + "\001"
                                    + map.get("iid") + "\001"
                                    + map.get("appid") + "\001"
                                    + map.get("version") + "\001"
                                    + map.get("appsessid") + "\001"
                                    + map.get("ccode") + "\001"
                                    + map.get("carrier") + "\001"
                                    + map.get("make") + "\001"
                                    + map.get("model") + "\001"
                                    + map.get("os") + "\001"
                                    + map.get("osver") + "\001"
                                    + map.get("newuser") + "\001"
                                    + map.get("oldver") + "\001"
                                    + map.get("cid") + "\001"
                                    + map.get("lastop") + "\001"
                                    + map.get("vidsessid") + "\001"
                                    + map.get("profilemap") + "\001"
                                    + map.get("profile") + "\001"
                                    + map.get("error") + "\001"
                                    + map.get("trainsessid") + "\001"
                                    + map.get("bw") + "\001"
                                    + map.get("nbuffs") + "\001"
                                    + map.get("buftime") + "\001"
                                    + map.get("ltime") + "\001"
                                    + map.get("nseeks") + "\001"
                                    + map.get("sessdur") + "\001"
                                    + map.get("tdur") + "\001"
                                    + map.get("mode") + "\001"
                                    + map.get("trigger") + "\001"
                                    + map.get("containerid") + "\001"
                                    + now.getTime() +"\001"
                                    + map.get("time") + "\001"
                                    + map.get("nonce") + "\001"
                                    + map.get("offset") + "\001"
                                    + map.get("ip") + "\001"
                                    + map.get("ua")+"\001"
                                    + map.get("site") + "\001"
                                    + map.get("country") + "\001"
                                    + map.get("vendor") + "\r\n";
                            System.out.println("**************"+rsl);
                            strAll.append(rsl);

                        }
                        index = index +1;
                        if(index > 0 && count > 0 && index == count) {
                            long t1 = System.currentTimeMillis();
                            String COPY_CMD = "";
                            String driver = "org.postgresql.Driver";
                            String url = "jdbc:postgresql://64.62.166.90:16100/ba";
                            String user = "vuclip2";
                            String password = "blue123";
                            if (map.get("pid").equals("305")){
                                COPY_CMD = "COPY page_vv_fact_test from STDIN DELIMITER AS '\001' LOG ERRORS INTO ua_error SEGMENT REJECT LIMIT 100 ROWS;";

                            }else {
                                COPY_CMD = "COPY page_fact_test from STDIN DELIMITER AS '\001' LOG ERRORS INTO ua_error SEGMENT REJECT LIMIT 100 ROWS;";

                            }
                            System.out.println("~# Loading Driver " + driver);
                            Class.forName(driver);
                            System.out.println("~# connecting to database with url " + url);
                            Connection con = DriverManager.getConnection(url, user, password);
                            CopyManager cm = new CopyManager((BaseConnection) con);
                            Reader reader  = new StringReader(strAll.toString());
                            System.out.println(strAll.toString()+"*************************");
                            cm.copyIn(COPY_CMD, reader);
                            System.out.println("~# COPY operation completed successfully");
                            con.close();
                            System.out.println("time diff:"+ (System.currentTimeMillis() - t1));
                            //ZkUtils.updatePersistentPath();
                            for (OffsetRange o : offsetRanges.get()) {
                                System.out.println(
                                        o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset()
                                );
                            }
                        }



                    }
                });
                return null;
            }
        });

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}
