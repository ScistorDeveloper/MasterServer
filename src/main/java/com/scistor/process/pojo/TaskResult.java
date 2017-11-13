package com.scistor.process.pojo;

import java.io.Serializable;
import java.util.List;

/**
 * Created by Administrator on 2017/11/10.
 */
public class TaskResult implements Serializable {

	private String taskId;
	private String mainClass;
	private boolean finished;
	private int errorCode;
	private List<String> errorInfo;

}
