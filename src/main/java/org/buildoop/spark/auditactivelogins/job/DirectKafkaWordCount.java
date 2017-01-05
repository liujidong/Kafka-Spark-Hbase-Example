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
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import scala.Tuple2;

import java.io.File;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount. Usage:
 * JavaDirectKafkaWordCount <brokers> <topics> <brokers> is a list of one or
 * more Kafka brokers <topics> is a list of one or more kafka topics to consume
 * from
 * <p>
 * Example: $ bin/run-example streaming.JavaDirectKafkaWordCount
 * broker1-host:port,broker2-host:port \ topic1,topic2
 */

public final class DirectKafkaWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");
    private static final Pattern COMMA = Pattern.compile("\":\"");
    private static final Pattern COLON = Pattern.compile(":");

    public static void main(String[] args) throws Exception {
//		if (args.length < 2) {
//			System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n"
//					+ "  <brokers> is a list of one or more Kafka brokers\n"
//					+ "  <topics> is a list of one or more kafka topics to consume from\n\n");
//			System.exit(1);
//		}

        // String brokers =
        // "64.71.156.203:2181,64.71.156.204:2181,64.71.156.205:2181";
        String brokers = "64.71.156.203:9092,64.71.156.204:9092,64.71.156.205:9092";
        String topics = "app_develop";

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JavaDirectKafkaWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        Set<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokers);

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
                StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                System.out.println("----------call--------" + tuple2);
                return tuple2._2().replaceFirst("\\d+\\{", "{");
            }
        });
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String x) {
                System.out.println("----------words--------" + x);
                List<String> ls = new ArrayList<String>();
                try {
                    JSONObject jsonObject = new JSONObject(x);
                    Iterator it = jsonObject.keys();
                    while (it.hasNext()) {
                        String key = String.valueOf(it.next());
                        ls.add(key);
                    }
                    JSONObject properties = (JSONObject) jsonObject.get("properties");
                    Iterator it2 = properties.keys();
                    while (it2.hasNext()) {
                        String key = String.valueOf(it2.next());
                        ls.add(key);
                    }
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                //return  Arrays.asList(COMMA.split(x));
                return ls;
            }
        });
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });
        wordCounts.foreach(new Function2<JavaPairRDD<String, Integer>, Time, Void>() {

            @Override
            public Void call(JavaPairRDD<String, Integer> values,
                             Time time) throws Exception {
                //values.saveAsTextFile("file:/d:/tmp/output.txt");
                String filename = (new SimpleDateFormat("yyyyMMddHHmmss")).format(new Date()) + ".txt";
                final File f = new File("d:/tmp/output/", filename);
                final long count = values.count();
                System.out.println("================count===================" + count);
                values.foreach(new VoidFunction<Tuple2<String, Integer>>() {
                    long index = 0;
                    StringBuffer strAll = new StringBuffer();

                    @Override
                    public void call(Tuple2<String, Integer> tuple)
                            throws Exception {
                        String result = tuple._1() + "\001" + tuple._2() + "\r\n";
                        //"------------------------------- Counter:" + tuple._1() + "," + tuple._2();
                        System.out.println(result);
                        strAll.append(result);
                        index = index + 1;
                        if (index == count) {
                            FileUtils.writeStringToFile(f, strAll.toString(), true);
                            long t1 = System.currentTimeMillis();
                            String driver = "org.postgresql.Driver";
                            String url = "jdbc:postgresql://64.62.166.90:16100/ba";
                            ;
                            String user = "vuclip2";
                            String password = "blue123";
                            String COPY_CMD = "COPY word_count from STDIN DELIMITER AS '\001';";
                            System.out.println("~# Loading Driver " + driver);
                            Class.forName(driver);
                            System.out.println("~# connecting to database with url " + url);
                            Connection con = DriverManager.getConnection(url, user, password);
                            CopyManager cm = new CopyManager((BaseConnection) con);
                            Reader reader = new StringReader(strAll.toString());
                            cm.copyIn(COPY_CMD, reader);
                            System.out.println("~# COPY operation completed successfully");
                            con.close();
                            System.out.println("time diff:" + (System.currentTimeMillis() - t1));
                        }
                    }
                });
                return null;
            }
        });
        wordCounts.print();

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}
