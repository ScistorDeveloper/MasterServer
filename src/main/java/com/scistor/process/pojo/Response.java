package com.scistor.process.pojo;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.List;

/**
 * @description reponse for request of thrift
 */
public class Response {
	/**
	 * @description response for task request,
	 * 	{taskId,errorCode,errorInfo}
	 */
	public static class TaskResponse{

		private int errorCode;
		private List<String> errorInfo;

		public TaskResponse(int errorCode, List<String> errorInfo) {
			super();
			this.errorCode = errorCode;
			this.errorInfo = errorInfo;
		}

		public String toJSON(){
			JSONObject obj=new JSONObject();
			obj.put("errorCode", errorCode);
			JSONArray JArray = new JSONArray();
			JArray.add(errorInfo);
			obj.put("errorInfo", JArray.toString());
			return obj.toString();
		}

		public int getErrorCode() {
			return errorCode;
		}

		public void setErrorCode(int errorCode) {
			this.errorCode = errorCode;
		}

		public List<String> getErrorInfo() {
			return errorInfo;
		}

		public void setErrorInfo(List<String> errorInfo) {
			this.errorInfo = errorInfo;
		}
		
	}

	/**
	 * 
	 * @description handler operator response...
	 * 	{componentName,errorCode,errorInfo}
	 *
	 */
	public static class OperatorResponse{

		private String componentName;
		private int errorCode;
		private List<String> errorInfo;

		public OperatorResponse(String componentName){
			this.componentName = componentName;
		}

		public OperatorResponse(String componentName, int errorCode,
		                        List<String> errorInfo) {
			super();
			this.componentName = componentName;
			this.errorCode = errorCode;
			this.errorInfo = errorInfo;
		}

		public String toJSON(){
			JSONObject obj = new JSONObject();
			obj.put("componentName", componentName);
			obj.put("errorCode", errorCode);
			JSONArray jArray=new JSONArray();
			jArray.add(errorInfo);
			obj.put("errorInfo", jArray.toString());
			return obj.toString();
		}

		public String getComponentName() {
			return componentName;
		}

		public void setComponentName(String componentName) {
			this.componentName = componentName;
		}

		public int getErrorCode() {
			return errorCode;
		}

		public void setErrorCode(int errorCode) {
			this.errorCode = errorCode;
		}

		public List<String> getErrorInfo() {
			return errorInfo;
		}

		public void setErrorInfo(List<String> errorInfo) {
			this.errorInfo = errorInfo;
		}

	}
}
