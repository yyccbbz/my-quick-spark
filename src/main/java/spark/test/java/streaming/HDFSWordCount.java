package spark.test.java.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: CuiCan
 * @Date: 2017-7-6
 * @Time: 11:17
 * @Description: 基于HDFS文件的实时wordcount程序
 */
public class HDFSWordCount {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("HDFSWordCount");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaDStream<String> lines = jsc.textFileStream("hdfs://devcluster/wordcount_dir");

//        JavaPairDStream<String, Integer> pairs0 =
//                lines.flatMap((FlatMapFunction<String, String>)words -> Arrays.asList(words.split(" ")).iterator())
//                        .mapToPair((PairFunction<String, String, Integer>)str -> new Tuple2<String, Integer>(str, 1));
//        JavaPairDStream<String, Integer> res = pairs0.reduceByKey((v1, v2) -> v1 + v2);
//        res.print();

        JavaDStream<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    private static final long serialVersionUID = 151522034013566982L;

                    @Override
                    public Iterable<String> call(String lines) throws Exception {
                        return Arrays.asList(lines.split(" "));
                    }
                });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    private static final long serialVersionUID = -3810571506789587767L;

                    @Override
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<>(word, 1);
                    }
                });

        JavaPairDStream<String, Integer> counts = pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    private static final long serialVersionUID = -8213563096786490666L;

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