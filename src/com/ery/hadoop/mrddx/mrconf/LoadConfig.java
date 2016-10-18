package com.ery.hadoop.mrddx.mrconf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ery.hadoop.mrddx.log.MRLog;

/**
 * 加载除HADOOP JOB之外的资源文件
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-2-1
 * @version v1.0
 */
public class LoadConfig {
	public static final Log LOG = LogFactory.getLog(LoadConfig.class);
	public static final String FILE_SUFFIX_PROPERTIES = ".properties";
	public static final String FILE_SUFFIX_XML = ".xml";

	/**
	 * 加载资源文件
	 * 
	 * @param resource
	 *            资源文件列表
	 * @return 资源内容
	 * @throws IOException
	 *             IO异常
	 */
	public Map<String, String> load(Set<String> resource) throws IOException {
		Map<String, String> content = new HashMap<String, String>();
		Iterator<String> iterator = resource.iterator();
		while (iterator.hasNext()) {
			String sfile = iterator.next();
			if (null != sfile && sfile.endsWith(FILE_SUFFIX_PROPERTIES)) {
				this.loadProperties(content, sfile);
				MRLog.info(LOG, "Load resource file<" + sfile + ">" + " finished!");
			}
		}

		return null;
	}

	/**
	 * 加载properties文件
	 * 
	 * @param content
	 *            返回结果列表
	 * @param sfile
	 *            文件
	 * @throws IOException
	 *             IO异常
	 */
	public void loadProperties(Map<String, String> content, String sfile) throws IOException {
		Properties props = new Properties();
		InputStream in = null;
		try {
			in = LoadConfig.class.getClassLoader().getResourceAsStream(sfile);
			props.load(in);
			Enumeration<?> en = props.propertyNames();
			while (en.hasMoreElements()) {
				Object key = en.nextElement();
				if (null != key && key instanceof String) {
					content.put((String) key, props.getProperty((String) key));
				}
			}
		} finally {
			if (null != in) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
