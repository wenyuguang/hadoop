package hadoop.kafka;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import hadoop.util.IpUtil;

public class HelloWorldProducer2 {
	public static void main(String[] args) throws InterruptedException {
		long events = 10000000;
		Properties props = new Properties();
		props.put("bootstrap.servers", "10.1.89.50:9092");
		props.put("acks", "0");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// 配置partitionner选择策略，可选配置
//		props.put("partitioner.class", "hadoop.kafka.SimplePartitioner2");

		Producer<String, String> producer = new KafkaProducer<>(props);

		ExecutorService executorService = new ThreadPoolExecutor(1, 30, 0L, TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<Runnable>());

		for (long nEvents = 0; nEvents < events; nEvents++) {
			executorService.execute(() -> {
				long runtime = new Date().getTime();
				String ip = IpUtil.getRandomIp();
				String msg = runtime + ",www.example.com," + ip;
				ProducerRecord<String, String> data = new ProducerRecord<String, String>("flume-data", ip, msg);
				producer.send(data, new Callback() {
					public void onCompletion(RecordMetadata metadata, Exception e) {
						if (e != null) {
							e.printStackTrace();
						} else {
							System.out.println("The offset of the record we just sent is: " + metadata.offset());
						}
					}
				});
			});
		}
		executorService.shutdown();
		boolean isDone = false;
		while (!isDone) {
			isDone = executorService.awaitTermination(5, TimeUnit.SECONDS);
		}
		producer.close();
	}
}