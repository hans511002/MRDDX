package com.ery.hadoop.mrddx.zk;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.ery.hadoop.mrddx.log.MRLog;

/**
 * 节点监听类
 * 



 * @createDate 2013-1-22
 * @version v1.0
 */
public class MRWatcher implements Watcher {
	// 日志对象
	public static final Log LOG = LogFactory.getLog(MRWatcher.class);
	public static final String RUNNING = "running"; // 运行中
	public static final String FAILED = "failed"; // 失败

	// 监听的时间类型
	public Watcher.Event.EventType eventType;

	// 监听类
	private IMonitorZKNode monitor;

	// zk对象
	private MRZooKeeper mrZooKeeper;

	// 任务节点路径
	private String zkTaskPath;

	/**
	 * 构造方法
	 * 
	 * @param mrZooKeeper
	 *            zk对象
	 * @param zkTaskPath
	 *            任务节点路径
	 */
	public MRWatcher(MRZooKeeper mrZooKeeper, String zkTaskPath) {
		this.eventType = Watcher.Event.EventType.None;
		this.mrZooKeeper = mrZooKeeper;
		this.zkTaskPath = zkTaskPath;
	}

	@Override
	public void process(WatchedEvent event) {
		if (null == this.monitor || null == this.zkTaskPath) {
			return;
		}

		String zkNodeData = null;
		boolean zkDelete = false;
		this.eventType = event.getType();

		try {
			zkNodeData = this.mrZooKeeper.getData(this.zkTaskPath, true);
		} catch (Exception e) {
			MRLog.errorZK(LOG, "Get taskNode data exception," + e.getMessage() + " EventType:" + this.eventType);
		}

		if (EventType.NodeDeleted.equals(this.eventType)) {
			zkDelete = true;
		} else {

			zkDelete = false;
		}

		this.monitor.setZKProcessNodeData(zkNodeData);
		this.monitor.setZkProcessIsDeleted(zkDelete);
	}

	public boolean nodeCreated() {
		return this.eventType == Watcher.Event.EventType.NodeCreated;
	}

	public boolean nodeDeleted() {
		return this.eventType == Watcher.Event.EventType.NodeDeleted;
	}

	public void setMoniter(IMonitorZKNode monitor) {
		this.monitor = monitor;
	}
}
