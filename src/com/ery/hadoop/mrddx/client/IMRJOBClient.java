package com.ery.hadoop.mrddx.client;

import java.util.Map;

/**
 * 客户端调用JOB接口
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-1-16
 * @version v1.0
 */
public interface IMRJOBClient {
	/**
	 * 运行job
	 * 
	 * @param param
	 *            运行job所需参数
	 */
	public void run(Map<String, String> paramMap) throws Exception;
}
