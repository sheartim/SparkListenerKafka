package com.amazonaws.awslabs.sparklistener;

import java.util.ArrayList;
//import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext;
//import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.dstream.InputDStream;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategy;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.LocationStrategy;

import com.amazonaws.awslabs.sparklistener.listener.CloudWatchSparkListener;

import scala.Tuple2;

//import scala.reflect.ClassTag;

public class SparkListenerKafka {

  public static void main(String[] args) throws InterruptedException {
    System.out.println("SparkListenerKafka main: " + args[0] + " " + args[1] + " " + args[2]);
    String appName = args[0];
    String bootstrapservers = args[1];
    String topic = args[2];
    //String outputPath = args[3];
    //String checkpointLocation = args[4];

    SparkConf conf = new SparkConf()
        .setAppName(appName)
        //.setMaster("local[*]")
        .set("spark.streaming.kafka.maxRatePerPartition", "100");
        //.set("spark.sql.autoBroadcastJoinThreshold=", "-1")
        //.set("spark.sql.broadcastTimeout","500000ms");

    SparkContext sc = new SparkContext(conf);

    StreamingContext streamingContext = new StreamingContext(sc, Seconds.apply(10));

    CloudWatchSparkListener cwListener = new CloudWatchSparkListener(appName);

    streamingContext.addStreamingListener(cwListener);

    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put("bootstrap.servers", bootstrapservers);
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    kafkaParams.put("group.id", "kafka_demo_group");
    kafkaParams.put("auto.offset.reset", "earliest");
    kafkaParams.put("enable.auto.commit", true);

    ArrayList<String> topics = new ArrayList<String>();
    topics.add(0, topic);
    LocationStrategy locationStrategy = LocationStrategies.PreferConsistent();
    ConsumerStrategy<String, String> consumerStrategy = ConsumerStrategies.Subscribe(topics, kafkaParams, Collections.emptyMap());

    InputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(streamingContext, locationStrategy, consumerStrategy);

    scala.reflect.ClassTag<ConsumerRecord<String, String>> classTag = scala.reflect.ClassTag$.MODULE$.apply(ConsumerRecord.class);
    JavaInputDStream<ConsumerRecord<String, String>> jstream = new JavaInputDStream<> (stream,  classTag);

    //JavaDStream<String> words = jstream.map(record -> record.value())
    //.flatMap(line -> Arrays.asList(line.split("[ ,\\.;:\\-]+")).iterator())
    //.map(word -> word.toLowerCase())
    //.filter(word -> word.length() > 0)
    //.mapToPair(word -> new Tuple2<>(word, 1));
    //.reduceByKey((a, b) -> a + b);
    //.repartition(1);
    
    JavaPairDStream<String, Integer> wordCounts = jstream.mapToPair(record -> new Tuple2<>(record.value(), 1))
          .reduceByKey((a, b) -> a + b);

    JavaPairDStream<Integer, String> sortedWordCounts = wordCounts.mapToPair(Tuple2::swap)
          .transformToPair(rdd -> rdd.sortByKey(false));

    JavaPairDStream<String, Integer> sortedWords = sortedWordCounts.mapToPair(Tuple2::swap);

    sortedWords.print();
    System.out.println("SparkListenerKafka: sortedWords.print()");  
    streamingContext.start();
    System.out.println("SparkListenerKafka: streamingContext.start()");  
    streamingContext.awaitTermination();
  }
}