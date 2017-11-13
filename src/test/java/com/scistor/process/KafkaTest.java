package com.scistor.process;

import kafka.admin.TopicCommand;

/**
 * Created by Administrator on 2017/11/10.
 */
public class KafkaTest {

	public static void main(String[] args) {
//		String[] options = new String[]{
//				"--create",
//				"--zookeeper","172.16.18.228:2999",
//				"--partitions", "3",
//				"--topic", "my_test_topic",
//				"--replication-factor", "2"
//		};
//		TopicCommand.main(options);

//		String[] options = new String[] {
//				"--list",
//				"--zookeeper","172.16.18.228:2999",
//		};
//		TopicCommand.main(options);

//		String[] options = new String[] {
//				"--delete",
//				"--zookeeper","172.16.18.228:2999",
//				"--topic", "my_test_topic" };
//		TopicCommand.main(options);

		String[] options = new String[] {
				"--describe",
				"--zookeeper","172.16.18.228:2999",
				"--topic", "HStest", };
		TopicCommand.main(options);

	}

}
