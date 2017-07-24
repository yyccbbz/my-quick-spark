package spark.test.java.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: CuiCan
 * @Date: 2017-7-5
 * @Time: 16:33
 * @Description: 实时WordCount程序
 */
public class WordCount implements Serializable {

    private static final long serialVersionUID = -5906971487779751764L;

    public static void main(String[] args) throws Exception {

        //创建配置conf对象，设置本地模式，线程为2
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("WordCount");

        //创建context对象
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(1));

        //创建输入DStream
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 999);

//        JavaPairDStream<String, Integer> pairs0 =
//                lines.flatMap((str) -> Arrays.asList(str.split(" ")).iterator())
//                        .mapToPair((str) -> new Tuple2<String, Integer>(str, 1L));
//        JavaPairDStream<String, Integer> res = pairs0.reduceByKey((v1, v2) -> v1 + v2);
//        res.print();


        //开始对接收的数据进行计算
        JavaDStream<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    private static final long serialVersionUID = 4305132911856782407L;

                    @Override
                    public Iterable<String> call(String line) throws Exception {
                        return Arrays.asList(line.split(" "));
                    }
                });

        //开始进行flatMap、reduceByKey操作
        JavaPairDStream<String, Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    private static final long serialVersionUID = -107756216407532332L;

                    @Override
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }
                });

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    private static final long serialVersionUID = -7379103059533326899L;

                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                });

        Thread.sleep(5000);
        wordCounts.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }

}
