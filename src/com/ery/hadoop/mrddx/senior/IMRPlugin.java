package com.ery.hadoop.mrddx.senior;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * 插件接口
 * 

 * 
 */
public abstract class IMRPlugin {
	// map中
	public abstract List<Map<String, Object>> mapPlugin(Map<String, Object> value) throws IOException;

	// 文本文件的MAP中
	public abstract String line(String value) throws IOException;

	public abstract void configurePlugin(TaskInputOutputContext context);

	public abstract void closePlugin() throws IOException;
}
