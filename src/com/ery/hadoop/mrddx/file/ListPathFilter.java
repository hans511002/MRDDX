package com.ery.hadoop.mrddx.file;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**


 * 

 * @version v1.0
 */
public class ListPathFilter implements PathFilter, Configurable {
	public static final Log LOG = LogFactory.getLog(ListPathFilter.class);
	// 常量标识符
	public static final String CONFIG_LISTPATHFILTER = "mapreduce.listpathfilter.paths";
	// 配置信息
	private Configuration conf;
	// 绝对路径列表
	private Set<String> absolutePathSet;

	@Override
	public boolean accept(Path path) {
		if (null == path) {
			return false;
		}

		LOG.info("path.getName():" + path.getName());
		return this.absolutePathSet.contains(path.getName());
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		this.absolutePathSet = new HashSet<String>();
		String[] FileName = conf.getStrings(CONFIG_LISTPATHFILTER);
		for (String name : FileName) {
			this.absolutePathSet.add(name);
		}
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}
}
