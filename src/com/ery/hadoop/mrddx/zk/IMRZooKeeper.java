package com.ery.hadoop.mrddx.zk;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

/**
 * ZK操作接口
 * 



 * @createDate 2013-1-22
 * @version v1.0
 */
public interface IMRZooKeeper {
	/**
	 * 关闭资源
	 * 
	 * @throws InterruptedException
	 */
	public void destroy() throws InterruptedException;

	/**
	 * 设置节点的值
	 * 
	 * @param path
	 *            节点路径
	 * @param data
	 *            值
	 * @throws KeeperException
	 *             zk异常
	 * @throws InterruptedException
	 *             打断异常
	 */
	public void setData(String path, byte[] data) throws KeeperException, InterruptedException;

	/**
	 * 获取节点的数据
	 * 
	 * @param path
	 *            节点路径
	 * @param watch
	 *            是否监听
	 * @param stat
	 *            状态
	 * @return 数据
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public String getData(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException;

	/**
	 * 
	 * 获取节点的数据
	 * 
	 * @param path
	 *            节点路径
	 * @param watch
	 *            监听对象
	 * @return 数据
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public String getData(String path, boolean watch) throws KeeperException, InterruptedException;

	/**
	 * 获取节点的数据
	 * 
	 * @param path
	 *            节点路径
	 * @param watch
	 *            监听对象
	 * @param stat
	 *            状态
	 * @return 数据
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public String getData(String path, Watcher watch, Stat stat) throws KeeperException, InterruptedException;

	/**
	 * 获取节点的数据
	 * 
	 * @param path
	 *            节点路径
	 * @param watch
	 *            监听对象
	 * @return 数据
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public String getData(String path, Watcher watch) throws KeeperException, InterruptedException;

	/**
	 * 新增FTP节点
	 * 
	 * @param path
	 *            FTP节点路径
	 * @param data
	 *            节点数据
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void apendFTPNode(String path, String data) throws KeeperException, InterruptedException;
	
	/**
	 * 新增Mapper节点
	 * 
	 * @param path
	 *            Mapper节点路径
	 * @param data
	 *            节点数据
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void apendMapperNode(String path, String data) throws KeeperException, InterruptedException;

	/**
	 * 新增Reduce节点
	 * 
	 * @param path
	 *            Reduce节点路径
	 * @param data
	 *            节点数据
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void apendReduceNode(String path, String data) throws KeeperException, InterruptedException;
	
	/**
	 * 新增处理文件的JOBID节点
	 * 
	 * @param path
	 *            JOBID节点路径
	 * @param data
	 *            节点数据
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void apendJobIdNode(String path, String data) throws KeeperException, InterruptedException;
	
	/**
	 * 新增Remote节点
	 * 
	 * @param path
	 *            Reduce节点路径
	 * @param data
	 *            节点数据
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void apendRemoteNode(String path, String data) throws KeeperException, InterruptedException;

	/**
	 * 新增节点
	 * 
	 * @param path
	 *            节点路径
	 * @param data
	 *            节点数据
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void apendNode(String path, String data) throws KeeperException, InterruptedException;

	/**
	 * 新增临时节点
	 * 
	 * @param path
	 *            节点路径
	 * @param data
	 *            节点数据
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void apendTempNode(String path, String data) throws KeeperException, InterruptedException;

	/**
	 * 新增临时节点
	 * 
	 * @param path
	 *            节点路径
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void apendTempNode(String path) throws KeeperException, InterruptedException;

	/**
	 * 删除节点
	 * 
	 * @param path
	 *            节点路径
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void delNode(String path) throws KeeperException, InterruptedException;

	/**
	 * 判断节点是否存在
	 * 
	 * @param path
	 *            节点路径
	 * @return true:存在,否则不存在
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public boolean exist(String path) throws KeeperException, InterruptedException;

	/**
	 * 获取MRWather对象
	 * 
	 * @return MRWather对象
	 */
	public MRWatcher getWatcher();

	/**
	 * 获取路径下的子节点
	 * 
	 * @param path
	 *            父节点路径
	 * @return 子节点列表
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public List<String> getChildren(String path) throws KeeperException, InterruptedException;
}
