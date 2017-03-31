package com.ery.hadoop.mrddx.client;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.service.MRJOBService;

/**
 * 客户端类
 * 



 * @createDate 2013-2-21
 * @version v1.0
 */
public class MRJOBClient implements IMRJOBClient {
	// 日志对象
	public static final Log LOG = LogFactory.getLog(MRJOBClient.class);

	@Override
	public void run(Map<String, String> paramMap) throws Exception {
		// 检查license
		// License.checkLicense();

		// 获取参数
		// 获取MR输入输出服务，并执行job
		Configuration conf = new Configuration();
		// 设置参数
		for (String key : paramMap.keySet()) {
			String value = paramMap.get(key);
			if (null != value) {// 转义符号
				value = value.replaceAll("\\\\n", "\n");
				value = value.replaceAll("\\\\r", "\r");
				conf.set(key, value);
				paramMap.put(key, value);
			}
		}

		String debug = paramMap.get(MRConfiguration.INTERNAL_JOB_LOG_DEBUG);
		if (null != debug) {
			String rownum = paramMap.get(MRConfiguration.INTERNAL_JOB_LOG_DEBUG_ROWNUM);
			conf.setInt(MRConfiguration.INTERNAL_JOB_LOG_DEBUG, Integer.parseInt(debug));
			conf.setInt(MRConfiguration.INTERNAL_JOB_LOG_DEBUG_ROWNUM, Integer.parseInt(rownum));
		}

		// 打印参数
		this.printParameter(paramMap);
		MRJOBService mrJobService = new MRJOBService();

		// 判断使用jobconf或者job执行
		Job job = Job.getInstance(conf);
		job.setJarByClass(MRJOBService.class);
		mrJobService.run(paramMap, job);

		// if (mrJobService.isJobRun(conf)) {
		// } else {
		// JobConf jobConf = new JobConf(conf, MRJOBService.class);
		// mrJobService.run(paramMap, jobConf);
		// }
	}

	/**
	 * 打印日志
	 * 
	 * @param paraMap 参数信息
	 */
	private void printParameter(Map<String, String> paraMap) {
		MRLog.systemOut("参数配置如下:");
		TreeSet<String> ts = new TreeSet<String>();
		ts.addAll(paraMap.keySet());
		Iterator<String> i = ts.iterator();
		while (i.hasNext()) {
			String key = i.next();
			System.out.println(key + ":" + paraMap.get(key));
		}
	}
}
