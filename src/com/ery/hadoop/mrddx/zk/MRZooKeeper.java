package com.ery.hadoop.mrddx.zk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * ZK操作类
 * 



 * @createDate 2013-1-22
 * @version v1.0
 */
public class MRZooKeeper implements IMRZooKeeper {
	private static final int SESSION_TIMEOUT = 500000;
	private ZooKeeper zk;
	private MRWatcher watcher;

	/**
	 * 构造方法
	 * 
	 * @param address
	 *            服务地址
	 * @param zkTaskPath
	 *            任务节点路径
	 * @throws IOException
	 *             IO异常
	 */
	public MRZooKeeper(String address, String zkTaskPath) throws IOException {
		this.watcher = new MRWatcher(this, zkTaskPath);
		this.zk = new ZooKeeper(address, SESSION_TIMEOUT, this.watcher);
	}

	/**
	 * 构造方法
	 * 
	 * @param address
	 *            服务地址
	 * @throws IOException
	 *             IO异常
	 */
	public MRZooKeeper(String address) throws IOException {
		this.zk = new ZooKeeper(address, SESSION_TIMEOUT, null);
	}

	@Override
	public MRWatcher getWatcher() {
		return watcher;
	}

	public void setWatcher(MRWatcher watcher) {
		this.watcher = watcher;
	}

	public void setData(String path, byte[] data) throws KeeperException, InterruptedException {
		this.zk.setData(path, data, -1);
	}

	@Override
	public String getData(String path, boolean watch) throws KeeperException, InterruptedException {
		byte[] data = this.zk.getData(path, watch, null);
		return null == data ? null : new String(data);
	}

	@Override
	public String getData(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
		byte[] data = this.zk.getData(path, watch, stat);
		return null == data ? null : new String(data);
	}

	@Override
	public String getData(String path, Watcher watch) throws KeeperException, InterruptedException {
		byte[] data = this.zk.getData(path, watch, null);
		return null == data ? null : new String(data);
	}

	@Override
	public String getData(String path, Watcher watch, Stat stat) throws KeeperException, InterruptedException {
		byte[] data = this.zk.getData(path, watch, stat);
		return null == data ? null : new String(data);
	}

	@Override
	public void apendNode(String path, String data) throws KeeperException, InterruptedException {
		this.zk.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}

	@Override
	public void apendTempNode(String path, String data) throws KeeperException, InterruptedException {
		this.zk.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}

	@Override
	public void apendFTPNode(String path, String data) throws KeeperException, InterruptedException {
		this.appendNode(path, data);
	}
	
	@Override
	public void apendMapperNode(String path, String data) throws KeeperException, InterruptedException {
		this.appendNode(path, data);
	}

	@Override
	public void apendReduceNode(String path, String data) throws KeeperException, InterruptedException {
		this.appendNode(path, data);
	}

	@Override
	public void apendJobIdNode(String path, String data) throws KeeperException, InterruptedException {
		this.appendNode(path, data);
	}
	
	@Override
	public void apendRemoteNode(String path, String data) throws KeeperException, InterruptedException {
		this.appendNode(path, data);
	}

	@Override
	public void apendTempNode(String path) throws KeeperException, InterruptedException {
		this.zk.create(path, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}

	@Override
	public void delNode(String path) throws KeeperException, InterruptedException {
		this.zk.delete(path, -1);
	}

	@Override
	public boolean exist(String path) throws KeeperException, InterruptedException {
		return this.zk.exists(path, true) == null ? false : true;
	}

	@Override
	public void destroy() throws InterruptedException {
		this.zk.close();
	}

	@Override
	public List<String> getChildren(String path) throws KeeperException, InterruptedException {
		if (this.zk.exists(path, true) == null){
			return new ArrayList<String>();
		}
		return this.zk.getChildren(path, false);
	}

	/**
	 * 递归创建节点
	 * 
	 * @param path
	 *            节点路径
	 * @param data
	 *            数据
	 * @throws KeeperException
	 *             zk异常
	 * @throws InterruptedException
	 *             打断异常
	 */
	private void appendNode(String path, String data) throws KeeperException, InterruptedException {
		StringBuilder strbuilder = new StringBuilder();
		char charsPath[] = path.toCharArray();
		for (int i = 0; i < charsPath.length; i++) {
			if (i != 0 && "/".equals(String.valueOf(charsPath[i]))) {
				String temp = strbuilder.toString();
				if (!this.exist(temp)) {
					this.apendNode(temp, data);
				}
				strbuilder.append(charsPath[i]);
			} else {
				strbuilder.append(charsPath[i]);
			}
		}

		if (!this.exist(path)) {
			this.apendNode(path, data);
		}
	}
}
