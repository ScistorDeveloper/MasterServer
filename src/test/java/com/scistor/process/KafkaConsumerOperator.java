package com.scistor.process;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.*;

public class KafkaConsumerOperator {
    /**
     * java实现Kafka消费者的示例
     * @author lisg
     *
     */
    private static final String TOPIC = "8d6aaf5e-c00b-4290-9a49-94c60d898dcd";
    private static final int THREAD_AMOUNT = 1;

	public static void main2(String[] args) {
		Properties kafkaProperties = new Properties();
		kafkaProperties.put("offsets.storage","zookeeper");
		kafkaProperties.put("bootstrap.servers", "g1:9092,g2:9092,HS:9092");
		kafkaProperties.put("zookeeper.connect", "g1:2999,g2:2999,HS:2999");
		kafkaProperties.put("zookeeper.connection.timeout.ms", "300000");
		kafkaProperties.put("zookeeper.session.timeout.ms", "300000");
		kafkaProperties.put("group.id", "xxx123");
		kafkaProperties.put("auto.offset.reset", "smallest");
		kafkaProperties.put("enable.auto.commit", "false");
		kafkaProperties.put("group.max.session.timeout.ms", "120000");
		kafkaProperties.put("request.timeout.ms", "300001"); //requst.timeout.ms must be large than session.timeout.ms
		kafkaProperties.put("session.timeout.ms", "300000"); //session.timeout.ms must be large than time spent on polled records handle
		//kafkaProperties.put("auto.commit.interval.ms","3000");
		kafkaProperties.put("fetch.message.max.bytes", "40960000");
		kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		Map<String,Integer> map=new HashMap<String, Integer>();
		map.put(TOPIC,1);
		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(kafkaProperties));
		Map<String, List<KafkaStream<byte[],byte[]>>> msgStreams = consumer.createMessageStreams(map);
		for(KafkaStream<byte[],byte[]> streamx:msgStreams.get(TOPIC)){
			ConsumerIterator<byte[],byte[]> it=streamx.iterator();
			while(it.hasNext()){
				MessageAndMetadata<byte[],byte[]> mm=it.next();
				String message=new String(mm.message());
				long offset=mm.offset();
				System.out.println(String.format("msg:%s,offset:%d", message,offset));
			}
		}
	}


    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props.put("zookeeper.connect", "g1:2999,g2:2999,HS:2999");
        props.put("group.id", "aa");
	    props.put("auto.offset.reset","smallest");
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "2000");
        props.put("auto.commit.interval.ms", "1000");

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(TOPIC, THREAD_AMOUNT);
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        Map<String, List<KafkaStream<byte[], byte[]>>> msgStreams = consumer.createMessageStreams(topicCountMap );
        List<KafkaStream<byte[], byte[]>> msgStreamList = msgStreams.get(TOPIC);
	    /*for(KafkaStream<byte[],byte[]> stream:msgStreamList){
		    ConsumerIterator<byte[],byte[]> it=stream.iterator();
		    while(it.hasNext()){
			    MessageAndMetadata<byte[],byte[]> mm=it.next();
			    String message=new String(mm.message());
			    long offset=mm.offset();
			    System.out.println(String.format("msg:%s,offset:%d", message,offset));
			    Thread.sleep(10000);
		    }
	    }*/

	    Thread[] threads = new Thread[msgStreamList.size()];
	    for (int i = 0; i < threads.length; i++) {
		    threads[i] = new Thread(new HanldMessageThread(msgStreamList.get(i), i));
		    threads[i].start();
	    }

	    System.out.println(String.format("Number of thread is [%s]", threads.length));

	    List<Long> lastRunnableTimeOrigin = new ArrayList<Long>(threads.length);
	    for (int i = 0; i < threads.length; i++) {
		    long currentTime = System.currentTimeMillis();
		    lastRunnableTimeOrigin.add(currentTime);
	    }

	    List<Long> lastRunnableTime = new ArrayList<Long>(threads.length);
	    for (int i = 0; i < threads.length; i++) {
		    long currentTime = System.currentTimeMillis();
		    lastRunnableTime.add(currentTime);
	    }

       /* while(true) {
            boolean b = ZookeeperOperator.checkPath("/HS/1/1");
	        if (!b) {
		        long start1 = System.currentTimeMillis();
	            long start = System.currentTimeMillis();
	            while (true) {
		            for (int i = 0; i < threads.length; i++) {
			            if (threads[i].getState().equals(Thread.State.RUNNABLE)) {
				            lastRunnableTime.set(i, System.currentTimeMillis());
			            }
		            }
		            long end = System.currentTimeMillis();
		            if (end - start > 1000) {
			            boolean flag = true;
			            for (int i = 0; i < threads.length; i++) {
				            if (lastRunnableTimeOrigin.get(i).equals(lastRunnableTime.get(i))) {
					            flag = flag & true;
				            } else {
					            flag = flag & false;
				            }
			            }
			            if (flag == true) {
				            break;
			            } else {
				            for (int i = 0; i < threads.length; i++) {
					            long time = lastRunnableTime.get(i);
					            lastRunnableTimeOrigin.set(i, time);
					            start = System.currentTimeMillis();
				            }
			            }
		            }
	            }
                if (consumer != null) {
                    consumer.shutdown();
                }
	            long end1 = System.currentTimeMillis();
	            System.out.println(String.format("time waited:[%s]", end1 - start1));
	            break;
            }
        }*/

    }

}

/**
 * 具体处理message的线程
 * @author Administrator
 *
 */
class HanldMessageThread implements Runnable {

    private KafkaStream<byte[], byte[]> kafkaStream = null;
    private int num = 0;

    public HanldMessageThread(KafkaStream<byte[], byte[]> kafkaStream, int num) {
        super();
        this.kafkaStream = kafkaStream;
        this.num = num;
    }

    public void run() {
	    System.out.println("begin.......");
        ConsumerIterator<byte[], byte[]> iterator = kafkaStream.iterator();
        while(iterator.hasNext()) {
            String message = new String(iterator.next().message());
            System.out.println("Thread no offset: " + num + ", message: " + message);
        }
        System.out.println("end.......");
    }

}
