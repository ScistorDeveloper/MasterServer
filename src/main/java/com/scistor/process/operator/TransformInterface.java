package com.scistor.process.operator;

import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2017/11/7.
 */
public interface TransformInterface {

	void init(Map<String,String> config); //初始化参数
	List<String> validate();//参数校验
	int process(Map<String,String> data);//编写业务处理逻辑
	int merge();//编写聚合逻辑
	void close();//关闭所用的资源

}
