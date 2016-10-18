package com.ery.hadoop.mrddx.zk;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;

import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.log.MRLog;

public class MRFileZookeeper {
	
	// 日志对象
	public static final Log LOG = LogFactory.getLog(MRFileZookeeper.class);
	
	// file jobid的zk节点
	private String fileJobIdZKNodePath;
	
	// file jobid的zk对象
	private MRZooKeeper fileJobIdZK;
	
	private String zkAddress;
	
	private String zkRootPath;
	
	// conf
	private Configuration mrconf;
	
	private boolean isEffective;
	
	public MRFileZookeeper(Configuration mrconf) {
		this.mrconf = mrconf;
		this.isEffective = mrconf.getBoolean(MRConfiguration.INTERNAL_SYS_ZK_FILE_LOCK_FLAG, true);
	}
	
	public void init() throws IOException, KeeperException, InterruptedException{
		if (null == mrconf){
			return;
		}
		
		if (!this.isEffective){
			return;
		}
		
		this.zkRootPath = mrconf.get(MRConfiguration.SYS_ZK_ROOT_PATH, "/mrddx");
		this.zkAddress = mrconf.get(MRConfiguration.SYS_ZK_ADDRESS, null);
		if (null == this.zkAddress){
			return;
		}
		
		String fileJobIdZKNodeRootPath = this.zkRootPath + "/" + "filejob";
		// 添加jobid zk临时节点
		this.fileJobIdZKNodePath = fileJobIdZKNodeRootPath + "/" + mrconf.getLong(MRConfiguration.SYS_JOB_ID, 0);
		this.fileJobIdZK = new MRZooKeeper(this.zkAddress, fileJobIdZKNodeRootPath);
		this.fileJobIdZK.apendJobIdNode(fileJobIdZKNodeRootPath, "");
		if (!this.fileJobIdZK.exist(this.fileJobIdZKNodePath)){
			this.fileJobIdZK.apendTempNode(this.fileJobIdZKNodePath, "");
			return;
		}
		
		this.mrconf.set(MRConfiguration.INTERNAL_SYS_ZK_JOB_JOBID_PATH, this.fileJobIdZKNodePath);
	}
	
	/**
	 * 标示为运行中
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public void lock() throws KeeperException, InterruptedException, IOException{
		if (!this.isEffective){
			return;
		}
		
		String nodedata = this.fileJobIdZK.getData(this.fileJobIdZKNodePath, true);
		if ("running".equals(nodedata)){
			throw new IOException("同一个处理任务，不能同时执行");
		}
		
		// 锁定任务节点
		this.fileJobIdZK.setData(this.fileJobIdZKNodePath, "running".getBytes());
	}
	
	/**
	 * 标示为解锁中
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public void unlock() throws KeeperException, InterruptedException, IOException{
		if (!this.isEffective){
			return;
		}
		
		if (this.fileJobIdZK.exist(this.fileJobIdZKNodePath)){
			// 解锁任务节点
			this.fileJobIdZK.setData(this.fileJobIdZKNodePath, "none".getBytes());
		}
	}
	
	/**
	 * 关闭资源
	 */
	public void close() {
		if (!this.isEffective){
			return;
		}
		
		if (null == this.fileJobIdZK) {
			return;
		}
		
		try {
			this.fileJobIdZK.destroy();
		} catch (InterruptedException e) {
			MRLog.error(LOG, "关闭mapper的zk节点异常.");
		}

		try {
			IMRZooKeeper mrzk = new MRZooKeeper(this.zkAddress, null);
			ZKUtil.deleteZKNode(mrzk, this.fileJobIdZKNodePath, LOG);// 删除job节点
			mrzk.destroy();
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
