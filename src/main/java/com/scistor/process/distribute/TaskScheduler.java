package com.scistor.process.distribute;

import com.scistor.process.operator.TransformInterface;
import com.scistor.process.pojo.RequestQueue;
import com.scistor.process.pojo.Response.TaskResponse;
import com.scistor.process.pojo.TaskDetail;
import com.scistor.process.utils.XMLHelper;
import com.scistor.process.utils.params.RunningConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @Title TaskScheduler
 * @Desc 任务分发类
 * @Author WANG Shenghua
 * @Date 2017/11/7 11:08
 */
public class TaskScheduler {

	private static final Log LOG = LogFactory.getLog(TaskScheduler.class);
	private static final int CONCURRENT = RunningConfig.TASK_CONCURRENT_NUM;
	public static boolean flag = false;
	public static ClassLoader classLoader = null;
	public static Integer taskcount = 0;
	public static RequestQueue m_queue = new RequestQueue();

	public static TaskResponse newTask(String taskId, String xmlContent) {
		TaskResponse taskresponse = null;
		try {
			//进行XML校验
			boolean result = XMLHelper.TASKXMLValidator(xmlContent);
			LOG.info(String.format("Validate XML Result: [%s]", result));
			if (result) {
				List<Map<String, String>> mainClass2ElementList = generateMainClass2ElementList(xmlContent, taskId); //Key:算子主类，Value:算子执行所需参数
//				TaskResponse tResponse = validateActions(mainClass2ElementList, taskId);
//				if (tResponse != null) {
//					return tResponse;
//				}
				LOG.info(String.format("Validate Completed! Pushing It Into Request Queue, Current Queue Size Is: [%s]", m_queue.size()));
				int res = m_queue.push(taskId, xmlContent);
				if(res==-1){
					taskresponse = new TaskResponse(taskId, -111, new ArrayList<String>(){
						private static final long serialVersionUID = 1L;
						{
							add("Too Many Requests, Push Task Into Request Queue Failed!");
						}
					});
					return taskresponse;
				}
				taskresponse = new TaskResponse(taskId, 0, null);
				return taskresponse;
			} else {
				List<String> errorInfo = new ArrayList<String>();
				errorInfo.add("TASKXML 校验失败");
				taskresponse = new TaskResponse(taskId, -1, errorInfo);
				return taskresponse;
			}
		} catch (Exception e) {
			StackTraceElement[] stackTraceElements = e.getStackTrace();
			String error = "";
			for(StackTraceElement stackTraceElement : stackTraceElements) {
				error = error + stackTraceElement.toString();
				LOG.error(stackTraceElement);
			}
			List<String> errorInfo = new ArrayList<String>();
			errorInfo.add(error);
			taskresponse = new TaskResponse(taskId, -1, errorInfo);
			return taskresponse;
		}
	}

	private static List<Map<String, String>> generateMainClass2ElementList(String xmlContent, String taskId) throws UnsupportedEncodingException, DocumentException {
		List<Map<String, String>> mainClass2Element = new ArrayList<Map<String, String>>();
		SAXReader reader = new SAXReader();
		Document document = reader.read(new ByteArrayInputStream(xmlContent.getBytes("utf-8")));
		Element root = document.getRootElement();
		List<Element> totalElements = (List<Element>) root.elements();
		for (Element element : totalElements) {
			if (element.getName().equals("action")) {
				String type = element.attributeValue("type");
				Element operator = element.element("operator");
				Element mainclass = operator.element("mainclass");
				Element parameters = operator.element("parameters");

				Map<String, String> paramMap = new HashMap<String, String>();
				paramMap.put("taskId", taskId);
				paramMap.put("actionName", element.attributeValue("name"));
				paramMap.put("mainclass", mainclass.getTextTrim());
				paramMap.put("type", type);
				paramMap.put("task_type", "producer");
				paramMap.put("topic", UUID.randomUUID().toString());
				for (Element e : (List<Element>) parameters.elements()) {
					String key = e.elementTextTrim("key");
					String value = e.elementTextTrim("value");
					paramMap.put(key, value);
				}
				mainClass2Element.add(paramMap);
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
	private static TaskResponse validateActions(List<Map<String, String>> mainClass2ElementList, String taskId) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
		TaskResponse taskResponse = null;
		for (Map<String, String> mainClass2Element : mainClass2ElementList) {
			String mainClass = mainClass2Element.get("mainclass");
			Class reflectClass = classLoader.loadClass(mainClass);
			Object reflectObject = reflectClass.newInstance();
			//进行类加载和参数校验
			TransformInterface entry = (TransformInterface) reflectObject;
			entry.init(mainClass2Element, new ArrayBlockingQueue<Map>(1));
			List<String> validateResult = entry.validate();
			if (validateResult != null) {
				taskResponse = new TaskResponse(taskId, -101, validateResult);
				return taskResponse;
			}
		}
		return taskResponse;
	}


	static{
		initClassLoader();
		Thread thread = new Thread(new RunningTasks());
		thread.setName("RunningTasks");
		thread.start();
		LOG.info("TaskScheduler pulling message in MessageQueue started....");
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

	private static class RunningTasks implements Runnable {
		@Override
		public void run() {
			while(true){
				if(taskcount < CONCURRENT){
					try {
						TaskDetail taskDetail = m_queue.pull();
						String taskId = taskDetail.getTaskId();
						String xmlContent = taskDetail.getXmlContent();
						List<Map<String, String>> mainClass2ElementList = generateMainClass2ElementList(xmlContent, taskId);
						DistributeControl distributeControl = new DistributeControl(taskId, mainClass2ElementList);
						Thread thread = new Thread(distributeControl);
						thread.setName(taskId);
						thread.start();
						synchronized (TaskScheduler.taskcount) {
							TaskScheduler.taskcount++;
							LOG.info(String.format("Add New Tasks Success! Running Tasks' Number: [%s]", taskcount));
						}
					} catch (Exception e) {
						StackTraceElement[] stackTraceElements = e.getStackTrace();
						for (StackTraceElement stackTraceElement : stackTraceElements) {
							LOG.error(stackTraceElement);
						}
					}
				}else{
					try {
						Thread.sleep(3000);
					} catch (InterruptedException e) {
						LOG.info(e);
					}
				}
			}
		}
	}

}
