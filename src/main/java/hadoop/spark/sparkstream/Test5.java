package hadoop.spark.sparkstream;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class Test5 {

	public static void main(String[] args) throws InterruptedException {
		// 接收数据的地址和端口
		final JavaPairRDD<String, Integer>[] lastRdd = new JavaPairRDD[1];

		SparkConf conf = new SparkConf().setMaster("local").setAppName("streamingTest");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");
		sc.setCheckpointDir("./checkpoint");
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(10));

		// kafka相关参数，必要！缺了会报错
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "150.0.2.44:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "test");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", true);

		Collection<String> topics = Arrays.asList("nginx-log");

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(ssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		// 注意这边的stream里的参数本身是个ConsumerRecord对象
		JavaPairDStream<String, Integer> counts = stream
				.flatMap(x -> Arrays.asList(x.value().toString().split(" ")).iterator())
				.mapToPair(x -> new Tuple2<String, Integer>(x, 1)).reduceByKey((x, y) -> x + y);
		// counts.print();

		JavaPairDStream<String, Integer> result = counts
				.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
						/**
						 * values:经过分组最后 这个key所对应的value，如：[1,1,1,1,1] state:这个key在本次之前之前的状态
						 */
						Integer updateValue = 0;
						if (state.isPresent()) {
							updateValue = state.get();
						}

						for (Integer value : values) {
							updateValue += value;
						}
						return Optional.of(updateValue);
					}
				});

		result.print();

		ssc.start();
		ssc.awaitTermination();
		ssc.close();
	}
}