package com.ery.hadoop.mrddx.before;

import org.apache.hadoop.conf.Configuration;

/**
 * 执行MAP REDUCE的预处理操作
 * @author wanghao
 *
 */
public interface IDBMRBefore {
	public boolean before(Configuration conf) throws Exception ;
	public boolean close();
}
