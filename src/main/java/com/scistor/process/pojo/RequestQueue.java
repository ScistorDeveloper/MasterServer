package com.scistor.process.pojo;

import com.scistor.process.utils.params.RunningConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class RequestQueue {

	private static final Log LOG = LogFactory.getLog(RequestQueue.class);

	private final int MAX_REQUEST = RunningConfig.TASK_REQUEST_MAX;
	private BlockingQueue<TaskDetail> queue = new LinkedBlockingQueue<TaskDetail>(MAX_REQUEST);
	
	public int size(){
		return queue.size();
	}
	
	public int push(String taskId, String xmlContent){
		if(size() > MAX_REQUEST){
			return -1;
		}
		this.queue.add(new TaskDetail(taskId, xmlContent));
		LOG.info(String.format("[Queue]: New Task Has Been Pushed Into The Queue, Waiting Request Number [%s], taskId[%s], xmlContent[%s]", size(), taskId, xmlContent));
		return 0;
	}
	
	public TaskDetail pull(){
		TaskDetail taskDetail = null;
		try {
			taskDetail = queue.take();
		} catch (InterruptedException e) {
			LOG.error(e);
			for(StackTraceElement ste:e.getStackTrace()){
				LOG.error(ste);
			}
		}
		 return taskDetail;
	}

	public void cancelTask(String taskId){
		List<TaskDetail> toCancel = new ArrayList<TaskDetail>();
		Iterator<TaskDetail> it = queue.iterator();
		TaskDetail taskDetail = null;
		String taskid = "";
		while(it.hasNext()){
			taskDetail = it.next();
			taskid = taskDetail.getTaskId();
			if(StringUtils.equals(taskId, taskid)){
				toCancel.add(taskDetail);
			}
		}
		LOG.info("Cancel Task In RequestQueue Which Has Not Been Executed, To Cancel:" + toCancel);
		queue.removeAll(toCancel);
	}

}
