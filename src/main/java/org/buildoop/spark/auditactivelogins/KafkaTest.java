package org.buildoop.spark.auditactivelogins;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters$;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
/**
 * Created by Administrator on 2016/5/30.
 */
public class KafkaTest {
    private static HashMap<String, String> kafkaParam = new HashMap<String, String>();
    private static KafkaCluster kafkaCluster = null;
    private static scala.collection.immutable.Set<String> immutableTopics = null;
    private static Broadcast<HashMap<String, String>> kafkaParamBroadcast = null;

    public static <K, V>scala.collection.immutable.Map<K, V> convert(java.util.Map<K, V> m) {
        return JavaConverters$.MODULE$.mapAsScalaMapConverter(m).asScala().toMap(
                scala.Predef$.MODULE$.<scala.Tuple2<K, V>>conforms()
        );
    }
    // kafka direct stream 初始化时使用的offset数据
    public static Map<TopicAndPartition, Long> getClusterOffset(Set<String> topicSet){
        Map<TopicAndPartition, Long> consumerOffsetsLong = new HashMap<TopicAndPartition, Long>();

        scala.collection.immutable.Map<String, String> scalaKafkaParam = convert(kafkaParam);
        //System.out.println("kafkaParamMap.size():"+scalaKafkaParam.size());

        // init KafkaCluster
        kafkaCluster = new KafkaCluster(scalaKafkaParam);
        scala.collection.mutable.Set<String> mutableTopics = JavaConversions.asScalaSet(topicSet);
        immutableTopics = mutableTopics.toSet();
        scala.collection.immutable.Set<TopicAndPartition> topicAndPartitionSet2 = kafkaCluster.getPartitions(immutableTopics).right().get();

        // 没有保存offset时（该group首次消费时）, 各个partition offset 默认为0
        if (kafkaCluster.getConsumerOffsets(kafkaParam.get("group.id"), topicAndPartitionSet2).isLeft()) {
            System.out.println("=============no offsets================");
            System.out.println(kafkaCluster.getConsumerOffsets(kafkaParam.get("group.id"), topicAndPartitionSet2).left().get());

            Set<TopicAndPartition> topicAndPartitionSet1 = JavaConversions.setAsJavaSet(topicAndPartitionSet2);

            for (TopicAndPartition topicAndPartition : topicAndPartitionSet1) {
                consumerOffsetsLong.put(topicAndPartition, 0L);
            }

        }
        // offset已存在, 使用保存的offset
        else {

            scala.collection.immutable.Map<TopicAndPartition, Object> consumerOffsetsTemp = kafkaCluster.getConsumerOffsets(kafkaParam.get("group.id"), topicAndPartitionSet2).right().get();

            Map<TopicAndPartition, Object> consumerOffsets = JavaConversions.mapAsJavaMap(consumerOffsetsTemp);

            Set<TopicAndPartition> topicAndPartitionSet1 = JavaConversions.setAsJavaSet(topicAndPartitionSet2);

            for (TopicAndPartition topicAndPartition : topicAndPartitionSet1) {
                Long offset = (Long)consumerOffsets.get(topicAndPartition);
                consumerOffsetsLong.put(topicAndPartition, offset);
                System.out.println("topic:" + topicAndPartition.topic() + " partition:" + topicAndPartition.partition()  + " offset:"+offset);
            }

        }
        return consumerOffsetsLong;
    }
    public static void saveClusterOffset(AtomicReference<OffsetRange[]> offsetRanges){
        for (OffsetRange o : offsetRanges.get()) {

            // 封装topic.partition 与 offset对应关系 java Map
            TopicAndPartition topicAndPartition = new TopicAndPartition(o.topic(), o.partition());
            Map<TopicAndPartition, Object> topicAndPartitionObjectMap = new HashMap<TopicAndPartition, Object>();
            topicAndPartitionObjectMap.put(topicAndPartition, o.untilOffset());

            // 转换java map to scala immutable.map

            scala.collection.immutable.Map<TopicAndPartition, Object> scalatopicAndPartitionObjectMap = convert(topicAndPartitionObjectMap);

            // 更新offset到kafkaCluster
            kafkaCluster.setConsumerOffsets(kafkaParamBroadcast.getValue().get("group.id"), scalatopicAndPartitionObjectMap);

            System.out.println(
                    o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset()
            );
        }
    }
    public static void main(String[] args) throws Exception {
        Set<String> topicSet = new HashSet<String>();
        topicSet.add("analytics_topic_test");

        kafkaParam.put("metadata.broker.list", "64.71.156.203:9092,64.71.156.204:9092,64.71.156.205:9092");
        kafkaParam.put("group.id", "spark_test");

        SparkConf sparkConf = new SparkConf().setAppName("tachyon-test-consumer").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
        kafkaParamBroadcast = jssc.sparkContext().broadcast(kafkaParam);
        //得到集群的offset
        Map<TopicAndPartition, Long> consumerOffsetsLong = getClusterOffset(topicSet);
        // create direct stream
        JavaInputDStream<String> message = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                String.class,
                kafkaParam,
                consumerOffsetsLong,
                new Function<MessageAndMetadata<String, String>, String>() {
                    public String call(MessageAndMetadata<String, String> v1) throws Exception {
                        return v1.message();
                    }
                }
        );

        // 得到rdd各个分区对应的offset, 并保存在offsetRanges中
        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();
        JavaDStream<String> javaDStream = message.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
            public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
                OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                offsetRanges.set(offsets);
                return rdd.map(new Function<String, String>() {
                    @Override
                    public String call(String s) throws Exception {
                        return s.replaceFirst("\\d+\\{","{");
                    }
                });
            }
        });

        // output
        javaDStream.foreachRDD(new Function<JavaRDD<String>, Void>() {

            public Void call(JavaRDD<String> v1) throws Exception {
                if (v1.isEmpty()) return null;

                //处理rdd数据，这里保存数据为hdfs的parquet文件
//                HiveContext hiveContext = SQLContextSingleton.getHiveContextInstance(v1.context());
//                DataFrame df = hiveContext.jsonRDD(v1);
//                df.save("/offset/test", "parquet", SaveMode.Append);
                System.out.println("==========rdd.count===========" + v1.count());

                saveClusterOffset(offsetRanges);
                return null;
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }
}
