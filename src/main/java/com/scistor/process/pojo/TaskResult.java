package com.scistor.process.pojo;

import java.io.Serializable;

/**
 * Created by Administrator on 2017/11/10.
 */

public class TaskResult implements Serializable {

	private String taskId;
	private String mainClass;
	private boolean finished;
	private String errorInfo;

	public TaskResult(String taskId, String mainClass, boolean finished, String errorInfo) {
		this.taskId = taskId;
		this.mainClass = mainClass;
		this.finished = finished;
		this.errorInfo = errorInfo;
	}

	public String getTaskId() {
		return taskId;
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	public String getMainClass() {
		return mainClass;
	}

	public void setMainClass(String mainClass) {
		this.mainClass = mainClass;
	}

	public boolean isFinished() {
		return finished;
	}

	public void setFinished(boolean finished) {
		this.finished = finished;
	}

	public String getErrorInfo() {
		return errorInfo;
	}

	public void setErrorInfo(String errorInfo) {
		this.errorInfo = errorInfo;
	}

}
