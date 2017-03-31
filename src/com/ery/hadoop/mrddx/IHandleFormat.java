package com.ery.hadoop.mrddx;

import org.apache.hadoop.mapreduce.Job;

/**
 * format的业务逻辑处理接口
 * 



 * @createDate 2013-1-17
 * @version v1.0
 */
public interface IHandleFormat {
	/**
	 * 业务逻辑处理方法(包括验证参数等)
	 * 
	 * @param conf 配置对象
	 * @throws Exception 异常对象
	 */
	public void handle(Job conf) throws Exception;
}
