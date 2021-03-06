package com.scistor.process;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.UUID;

/**
 * Created by Administrator on 2017/11/10.
 */
public class KafkaProducerOperator {

	public static void main(String[] args) throws InterruptedException {
		Properties props = new Properties();
		props.put("producer.type","sync");
		props.put("bootstrap.servers", "172.16.18.228:9092,172.16.18.229:9092,172.16.18.234:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("request.required.acks", "1");
		KafkaProducer producer = new KafkaProducer<String, String>(props);
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("5d1434d9-0e20-4d91-a3cd-b5078a7ef849", UUID.randomUUID().toString(), "test23_60");
		producer.send(record, new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception e) {
				if (e != null)
					System.out.println("the producer has a error:" + e.getMessage());
				else {
					System.out.println("The offset of the record we just sent is: " + metadata.offset());
					System.out.println("The partition of the record we just sent is: " + metadata.partition());
				}

			}
		});
		producer.close();
	}

}
