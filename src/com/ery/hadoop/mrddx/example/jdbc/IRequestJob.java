package com.ery.hadoop.mrddx.example.jdbc;

import java.util.Map;

/**
 * 请求job客户的对象接口
 * 



 * @createDate 2013-2-21
 * @version v1.0
 */
public interface IRequestJob {
	// 获取job参数
	public Map<String, String> getJobParameter();
}
