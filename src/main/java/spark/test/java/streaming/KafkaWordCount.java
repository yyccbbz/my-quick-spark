package spark.test.java.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: CuiCan
 * @Date: 2017-7-6
 * @Time: 11:17
 * @Description: 基于kafka Receiver模式的实时wordcount程序
 * 创建topic
 * bin/kafka-topics.sh --zookeeper 172.16.52.105:2181,172.16.52.106:2181,172.16.52.107:2181 --topic wordcount --replication-factor 1 --partitions 1 --create
 * 创建producer
 * bin/kafka-console-producer.sh --broker-list 172.16.52.127:9092,172.16.52.128:9092 --topic wordcount
 */
public class KafkaWordCount {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("KafkaWordCount");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        /**
         * 使用KafkaUtils.createStream()，创建kafka输入数据源的离散数据流对象
         * 需要四个参数：
         *      StreamingContext
         *      zookeeper地址串
         *      kafka的groupId
         *      指定了线程数的topicMap集合
         */
        HashMap<String, Integer> topicThreadMap = new HashMap<>();
        topicThreadMap.put("wordcount", 1);
        JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(
                jsc,
                "172.16.52.105:2181,172.16.52.106:2181,172.16.52.107:2181",
                "DefaultConsumerGroup",
                topicThreadMap);


        JavaDStream<String> words = lines.flatMap(
                new FlatMapFunction<Tuple2<String, String>, String>() {

                    private static final long serialVersionUID = 788752226353490404L;

                    @Override
                    public Iterable<String> call(Tuple2<String, String> tuple) throws Exception {
                        return Arrays.asList(tuple._2().split(" "));
                    }

                });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {

                    private static final long serialVersionUID = 5159812834638053487L;

                    @Override
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<>(word, 1);
                    }
                });

        JavaPairDStream<String, Integer> counts = pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {

                    private static final long serialVersionUID = -5583739782633024041L;

                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return (v1 + v2);
                    }
                });

        counts.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.stop();
    }
}