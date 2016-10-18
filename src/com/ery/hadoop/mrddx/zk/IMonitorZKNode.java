package com.ery.hadoop.mrddx.zk;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

/**
 * 监听Zookeeper Node接口
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-1-30
 * @version v1.0
 */
public interface IMonitorZKNode {
	/**
	 * 设置ZK监听Node后，回调后Node的值
	 * 
	 * @param data
	 *            节点的数据
	 */
	public void setZKProcessNodeData(String data);

	/**
	 * 设置Node的值
	 * 
	 * @param data
	 *            节点的数据
	 * @throws KeeperException
	 *             zk异常
	 * @throws InterruptedException
	 *             打断异常
	 */
	public void setZKNodeData(byte[] data) throws KeeperException, InterruptedException;

	/**
	 * 设置Node是否被删除
	 * 
	 * @param deleted
	 *            是否被删除(true:被删除)
	 */
	public void setZkProcessIsDeleted(boolean deleted);

	/**
	 * 开始监听任务
	 * 
	 * @throws IOException
	 *             IO异常
	 */
	public void monitorTask() throws IOException;
}
