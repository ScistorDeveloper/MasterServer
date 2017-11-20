package com.scistor.process.distribute;

import com.scistor.process.operator.Impl.WhiteListFilterOperator;
import com.scistor.process.operator.TransformInterface;
import com.scistor.process.pojo.Response.TaskResponse;
import com.scistor.process.utils.ZKOperator;
import com.scistor.process.utils.params.RunningConfig;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @Title OperatorScheduler
 * @Desc 任务分发类
 * @Author WANG Shenghua
 * @Date 2017/11/7 11:08
 */
public class OperatorScheduler {

	private static final Log LOG = LogFactory.getLog(OperatorScheduler.class);
	public static ClassLoader classLoader = null;

	public static TaskResponse addNewOperators(String xmlContent) {
		TaskResponse taskresponse = null;
		try {
			List<Map<String, String>> mainClass2ElementList = generateMainClass2ElementList(xmlContent); //Key:算子主类，Value:算子执行所需参数
			if (mainClass2ElementList.size() == 0) {
				List<String> errorInfo = new ArrayList<String>();
				errorInfo.add("本次添加算子个数为0，或添加的算子均存在于系统中不能重复添加");
				return new TaskResponse(0, errorInfo);
			}
			TaskResponse tResponse = validateActions(mainClass2ElementList);
			if (tResponse != null) {
				return tResponse;
			}
			DistributeControl distributeControl = new DistributeControl(mainClass2ElementList);
			Thread thread = new Thread(distributeControl);
			thread.setName("initOperators");
			thread.start();
			taskresponse = new TaskResponse(0, null);
			return taskresponse;
		} catch (Exception e) {
			StackTraceElement[] stackTraceElements = e.getStackTrace();
			String error = "";
			for(StackTraceElement stackTraceElement : stackTraceElements) {
				error = error + stackTraceElement.toString();
				LOG.error(stackTraceElement);
			}
			List<String> errorInfo = new ArrayList<String>();
			errorInfo.add(error);
			taskresponse = new TaskResponse(-1, errorInfo);
			return taskresponse;
		}
	}

	private static List<Map<String, String>> generateMainClass2ElementList(String xmlContent) throws IOException, DocumentException, KeeperException, InterruptedException {
		ZooKeeper zooKeeper = ZKOperator.getZookeeperInstance();
		List<String> runningOperators = ZKOperator.getRunningOperators(zooKeeper);
		zooKeeper.close();
		List<Map<String, String>> mainClass2Element = new ArrayList<Map<String, String>>();
		SAXReader reader = new SAXReader();
		Document document = reader.read(new ByteArrayInputStream(xmlContent.getBytes("utf-8")));
		Element root = document.getRootElement();
		List<Element> totalElements = (List<Element>) root.elements();
		for (Element element : totalElements) {
			if (element.getName().equals("action")) {
				Element operator = element.element("operator");
				Element mainclass = operator.element("mainclass");
				if (!runningOperators.contains(mainclass.getTextTrim())) {
					Element parameters = operator.element("parameters");
					Map<String, String> paramMap = new HashMap<String, String>();
					paramMap.put("actionName", element.attributeValue("name"));
					paramMap.put("mainclass", mainclass.getTextTrim());
					paramMap.put("task_type", "producer");
					for (Element e : (List<Element>) parameters.elements()) {
						String key = e.elementTextTrim("key");
						String value = e.elementTextTrim("value");
						paramMap.put(key, value);
					}
					mainClass2Element.add(paramMap);
				}
			}
		}
		return mainClass2Element;
	}

	/**
	 * @Title validateActions
	 * @Desc 对各个要执行的算子进行参数校验
	 * @Param mainClass2ElementMap 算子主类和所需参数的映射
	 * @Author WANG Shenghua
	 * @Date 2017/11/7 10:04
	 * @Return TaskResponse 校验结果
	 */
	private static TaskResponse validateActions(List<Map<String, String>> mainClass2ElementList) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
		TaskResponse taskResponse = null;
		for (Map<String, String> mainClass2Element : mainClass2ElementList) {
			String mainClass = mainClass2Element.get("mainclass");
			Class reflectClass = classLoader.loadClass(mainClass);
			Object reflectObject = reflectClass.newInstance();
			//进行类加载和参数校验
			TransformInterface entry = (TransformInterface) reflectObject;
			entry.init(mainClass2Element, null);
			List<String> validateResult = entry.validate();
			if (validateResult != null) {
				taskResponse = new TaskResponse(-101, validateResult);
				return taskResponse;
			}
		}
		return taskResponse;
	}


	static{
		initClassLoader();
	}

	private static void initClassLoader() {
		File compenents = new File(RunningConfig.COMPONENT_LOCATION);
		File[] files = compenents.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".jar");
			}
		});
		URL[] urls = new URL[files == null || files.length == 0 ? 0 : files.length];
		for(int i = 0; i < (files == null ? 0 : files.length); i++){
			try {
				urls[i] = files[i].toURI().toURL();
			} catch (MalformedURLException e) {
				LOG.error(e);
			}
		}
		if(urls == null || urls.length == 0){
			classLoader = Thread.currentThread().getContextClassLoader();
		}else{
			classLoader = new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
		}
	}

}
