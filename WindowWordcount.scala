import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public class WindowWordcount {
	public static void main(String[] args) throws InterruptedException {
		Collection<String> topics = Arrays.asList("A");
		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put("group.id", "mygroup");
		kafkaParams.put("auto.offset.reset", "latest");
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Windowing");
		JavaStreamingContext jsc = new JavaStreamingContext(conf, new Duration(10000));
		JavaInputDStream<ConsumerRecord<String, String>> rawmessages = KafkaUtils.createDirectStream(jsc,
				LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, kafkaParams));
		JavaDStream<String> window_messages = rawmessages.map(record -> record.value());
		JavaDStream<String> messages = window_messages.window(new Duration(300000), new Duration(10000));
		jsc.checkpoint("/Users/nagainelu/bigdata/jobs/checkpoint");
		messages.print();
		jsc.start();
		jsc.awaitTermination();
	}
}
