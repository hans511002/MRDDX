package com.ery.hadoop.mrddx.zk;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.zookeeper.KeeperException;

import com.ery.hadoop.mrddx.log.MRLog;

public class ZKUtil {

	/**
	 * 删除zk路径下的子节点
	 * 
	 * @param mrZK
	 *            zk对象
	 * @param path
	 *            路径
	 * @throws KeeperException
	 *             zk异常
	 * @throws InterruptedException
	 *             线程打断异常
	 */
	public static void deleteZKChildNode(IMRZooKeeper mrZK, String path, Log log) throws KeeperException, InterruptedException {
		if (null == path || null == mrZK) {
			return;
		}

		List<String> children = mrZK.getChildren(path);
		if (null == children) {

			return;
		}

		for (String cpath : children) {
			cpath = path + "/" + cpath;
			if (mrZK.exist(cpath)) {
				mrZK.delNode(cpath);
				if (null != log) {
					MRLog.info(log, "删除zk节点路径, path:" + cpath);
				} else {
					MRLog.systemOut("删除zk节点路径, path:" + cpath);
				}
			}
		}
	}

	/**
	 * 删除zk节点
	 * 
	 * @param mrZK
	 *            zk对象
	 * @param path
	 *            路径
	 * @throws KeeperException
	 *             zk异常
	 * @throws InterruptedException
	 *             线程打断异常
	 */
	public static void deleteZKNode(IMRZooKeeper mrZK, String path, Log log) throws KeeperException, InterruptedException {
		if (null == path || null == mrZK) {
			return;
		}

		if (mrZK.exist(path)) {
			mrZK.delNode(path);
			if (null != log) {
				MRLog.info(log, "删除zk节点路径, path:" + path);
			} else {
				MRLog.systemOut("删除zk节点路径, path:" + path);
			}
		}
	}
}
