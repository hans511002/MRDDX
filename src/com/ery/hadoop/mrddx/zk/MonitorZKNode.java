package com.ery.hadoop.mrddx.zk;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import com.ery.hadoop.mrddx.log.MRLog;

/**
 * 负责处理zk节点的监听
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-2-18
 * @version v1.0
 */
public class MonitorZKNode implements IMonitorZKNode {
	public static final Log LOG = LogFactory.getLog(MonitorZKNode.class);

	// 节点的data数据
	private String zkResult;

	// 节点是否被删除(true:被删除)
	@SuppressWarnings("unused")
	private boolean zkDeleted;

	// zk的任务临时节点是否是当前对象创建.(true:是当前对象创建)
	private boolean isCreatedBySelf;

	// 当前map是否需要等待
	private boolean isNeedWait;

	// 任务id
	private String taskId;

	// 任务zk路径
	private String zkTaskPath;

	// ZK地址
	private String zkAddress;

	// 当前JOB的监控zk, 监控根路径为"/mrddx/jobname/mapper"
	private IMRZooKeeper mrZooKeeper;

	// 是否打印过wait日志
	private boolean isPrintWaitLog;

	/**
	 * 构造方法
	 * 
	 * @param taskId
	 *            任务id
	 * @param zkTaskPath
	 *            zk任务节点路径
	 * @param zkAddress
	 *            zk地址
	 */
	public MonitorZKNode(String taskId, String zkTaskPath, String zkAddress) {
		this.taskId = taskId;
		this.zkTaskPath = zkTaskPath;
		this.zkAddress = zkAddress;
	}

	@Override
	public void setZKNodeData(byte[] data) throws KeeperException, InterruptedException {
		this.mrZooKeeper.setData(this.zkTaskPath, data);
	}

	@Override
	public void setZKProcessNodeData(String data) {
		this.zkResult = data;
	}

	@Override
	public void setZkProcessIsDeleted(boolean deleted) {
		this.zkDeleted = deleted;
	}

	@Override
	public void monitorTask() throws IOException {
		// 任务节点是自己创建，直接返回
		if (this.isCreatedBySelf) {
			return;
		}

		// 创建节点
		if (null == this.mrZooKeeper) {
			this.mrZooKeeper = new MRZooKeeper(this.zkAddress, this.zkTaskPath);
			MRLog.infoZK(LOG, "create map task node success，node path:" + this.zkTaskPath);
		}

		try {
			// 注册监听
			MRWatcher watcher = this.mrZooKeeper.getWatcher();
			watcher.setMoniter(this);

			// 若节点不存在，则创建，并返回
			if (!this.mrZooKeeper.exist(this.zkTaskPath)) {
				this.mrZooKeeper.apendTempNode(this.zkTaskPath, MRWatcher.RUNNING);
				this.isCreatedBySelf = true;
				MRLog.infoZK(LOG, "mapper created tempNode, isCreatedBySelf:" + this.isCreatedBySelf);
				return;
			}

			// 创建节点的Mapper的task还没有结束时，另一个task被启动时
			this.zkResult = this.mrZooKeeper.getData(this.zkTaskPath, true);
			MRLog.infoZK(LOG, "New mapper task already started, The data of mapper's tempNode, zkResult:" + this.zkResult);
			while (true) {
				if (!this.isNeedWait && MRWatcher.RUNNING.equals(this.zkResult)) {
					this.isNeedWait = true;

					// 只打印一次日志
					if (!this.isPrintWaitLog) {
						this.isPrintWaitLog = true;
						MRLog.infoZK(LOG, "New mapper task is waiting");
					}
				}

				if (MRWatcher.FAILED.equals(this.zkResult)) {
					MRLog.errorZK(LOG, "New mapper task failed, throw exception!");
					throw new IOException("New mapper task failed!");
				}

				// 运行中,等待
				if (this.isNeedWait) {
					Thread.sleep(100);
					continue;
				}
			}
		} catch (Exception e) {
			MRLog.errorExceptionZK(LOG, "monitor zookeeper taskNode exception!", e);
			throw new IOException(e);
		}
	}

	/**
	 * 测试使用的临时方法
	 * 
	 * @throws IOException
	 */
	public void valida() throws IOException {
		if (!isCreatedBySelf) {
			MRLog.systemOut("!isCreatedBySelf");
			return;
		}

		if ((this.taskId.endsWith("000023") || this.taskId.endsWith("000005"))) {
			MRLog.systemOut("模拟IO异常");
			throw new IOException("模拟IO异常");
		}
	}
}
