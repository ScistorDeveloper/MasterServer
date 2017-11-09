package com.scistor.process;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.*;

public class KafkaConsumerOperator {
    /**
     * java实现Kafka消费者的示例
     * @author lisg
     *
     */
    private static final String TOPIC = "HStest";
    private static final int THREAD_AMOUNT = 1;

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props.put("zookeeper.connect", "172.16.18.234:2999");
        props.put("group.id", "HHS");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(TOPIC, THREAD_AMOUNT);
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        Map<String, List<KafkaStream<byte[], byte[]>>> msgStreams = consumer.createMessageStreams(topicCountMap );
        List<KafkaStream<byte[], byte[]>> msgStreamList = msgStreams.get(TOPIC);

	    Thread[] threads = new Thread[msgStreamList.size()];
	    for (int i = 0; i < threads.length; i++) {
		    threads[i] = new Thread(new HanldMessageThread(msgStreamList.get(i), i));
		    threads[i].start();
	    }

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

        while(true) {
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
        }

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
//            System.out.println("Thread no: " + num + ", message: " + message);
        }
        System.out.println("end.......");
    }

}
