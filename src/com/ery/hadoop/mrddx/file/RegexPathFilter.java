package com.ery.hadoop.mrddx.file;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * 正则表达式过滤器
 * 



 * @createDate 2013-1-14
 * @version v1.0
 */
public class RegexPathFilter implements PathFilter, Configurable {
	// 配置信息
	public Configuration conf;

	// 正则表达式
	private String regex;

	// true：处理匹配正则表达式的文件，否则，不处理匹配正则表达式的文件
	private boolean regexType;

	@Override
	public boolean accept(Path path) {
		if (null == path) {
			return false;
		}

		try {
			FileSystem fileSystem = path.getFileSystem(conf);
			if (fileSystem.getFileStatus(path).isDir()) {
				return true;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		// 若过滤的正则表达式未设置，返回true.
		if (null == this.regex || this.regex.trim().length() <= 0) {
			return true;
		}

		boolean match = path.getName().matches(this.regex);
		return this.regexType ? match : !match;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		this.regex = conf.get(FileConfiguration.INPUT_FILE_REGEXPATHFILTER);
		this.regexType = conf.getBoolean(FileConfiguration.INPUT_FILE_REGEXPATHFILTER_TYPE, true);
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}
}
