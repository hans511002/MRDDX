package com.ery.hadoop.mrddx;


/**
 * 监听配置接口
 * 



 * @version v1.0
 * @create Data 2013-2-1
 */
public interface IDBMonitor {
	/**
	 * 设置监听参数
	 * 
	 * @param conf
	 *            配置对象
	 */
	public void monitor(MRConfiguration conf);
}
