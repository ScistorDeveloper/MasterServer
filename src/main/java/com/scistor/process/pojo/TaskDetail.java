package com.scistor.process.pojo;

/**
 * Created by Administrator on 2017/11/7.
 */
public class TaskDetail {

	private String taskId;
	private String xmlContent;

	public TaskDetail(String taskId, String xmlContent) {
		this.taskId = taskId;
		this.xmlContent = xmlContent;
	}

	public String getTaskId() {
		return taskId;
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	public String getXmlContent() {
		return xmlContent;
	}

	public void setXmlContent(String xmlContent) {
		this.xmlContent = xmlContent;
	}

	@Override
	public String toString() {
		return "TaskDetail{" +
				"taskId='" + taskId + '\'' +
				", xmlContent='" + xmlContent + '\'' +
				'}';
	}

}
