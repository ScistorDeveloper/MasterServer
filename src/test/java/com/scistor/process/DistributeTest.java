package com.scistor.process;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2017/11/9.
 */
public class DistributeTest {

	private static int index = 0;

	public static void main(String[] args) {

		for (int m = 0; m <2; m++) {

			List<Map<String, String>> userDefinedOperaterList = new ArrayList<Map<String, String>>();
			//定义四个用户自定义算子
			for (int i = 0; i < 4; i++) {
				Map<String, String> map = new HashMap<String, String>();
				map.put("a", "a");
				userDefinedOperaterList.add(map);
			}

			List<Map<String, String>> mainClass2ElementList = new ArrayList<Map<String, String>>();
			//定义六个用户自定义算子
			for (int i = 0; i < 6; i++) {
				Map<String, String> map = new HashMap<String, String>();
				map.put("a", "a");
				mainClass2ElementList.add(map);
			}

			int current = 0;
			for (int i = 0; i < 8; i++) {
				final int slaveNo = i;
				if (current < userDefinedOperaterList.size() && index % 8 == slaveNo) {
					Map<String, String> curMergeOperator = userDefinedOperaterList.get(current);
					curMergeOperator.put("task_type", "consumer");
					mainClass2ElementList.add(curMergeOperator);
				}

				System.out.println("---------- " + i + " ----------");
				for (Map<String, String> map : mainClass2ElementList) {
					System.out.println(map);
				}

				if (current < userDefinedOperaterList.size() && index % 8 == slaveNo) {
					mainClass2ElementList.remove(mainClass2ElementList.size() - 1);
					current++;
					index++;
				}

				System.out.println("index = " + index);
			}

		}



	}

}
