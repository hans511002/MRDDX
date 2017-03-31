package com.ery.hadoop.mrddx.example;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.ery.hadoop.mrddx.DBGroupReducer;
import com.ery.hadoop.mrddx.DBMapper;
import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.DBReducer;
import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.file.FileConfiguration;
import com.ery.hadoop.mrddx.file.RegexPathFilter;
import com.ery.hadoop.mrddx.file.TextOutputFormat;
import com.ery.hadoop.mrddx.hive.HiveConfiguration;
import com.ery.hadoop.mrddx.hive.HiveInputFormat;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.HDFSUtils;

/**

 * 

 * @description
 * @date 2012-11-15
 */
public class MRHiveToFileService extends MRTestJOBService {
	public void run(Map<String, String> paraMap) throws Exception {
		// 获取系统参数
		String pResource[] = this.getParamSysResource(paraMap);// 获取资源文件
		String pJobName = this.getParamSysJobName(paraMap);// 获取job名称
		JobPriority pJobPriority = this.getParamSysJobPriority(paraMap);// 获取job优先级
		String pHdfsBaseUrl = this.getParamSysHdfsBaseUrl(paraMap);// 获取hdfsBaseUrl文件系统地址
		String pJobTrackerHost = this.getParamSysJobTrackerHost(paraMap);// 获取jobhost
		String pDirInput = this.getParamSysDirInput(paraMap);// 获取输入路径
		String pDirOutput = this.getParamSysDirOutput(paraMap);// 获取输出路径
		boolean pIsFilter = this.getParamSysIsFilter(paraMap);// 获取是否启用文件过滤标识
		String pRegFilterFile = this.getParamSysRegFilterFile(paraMap);// 获取过滤的正则表达式
		boolean pRegFilterType = this.getParamSysRegFilterType(paraMap);// 获取过滤类型

		String pInputHiveDBUrl = this.getParamInputHiveDBUrl(paraMap);// 获取Hive数据库地址
		String pInputHiveDBUserName = this.getParamInputHiveDBUserName(paraMap);// 获取Hive数据库用户名
		String pInputHiveDBPasswd = this.getParamInputHiveDBPasswd(paraMap);// 获取Hive数据库密码

		// 输入参数
		String pInputFieldNames = this.getParamInputFieldNames(paraMap);// 获取源字段
		String pGroupFieldMethod = this.getParamRelatedGroupFieldMethod(paraMap);// 获取目标字段:源字段:统计方法
		boolean pIsCombinerClass = this.getParamInputIsCombinerClass(paraMap);// 获取是否在map中合并
		String pInputQuery = this.getParamInputQuery(paraMap);// 获取输入的查询sql

		// 输出参数
		String pTargetFieldNames[] = this.getParamOutputFieldNames(paraMap);// 获取目标字段
		boolean pTargetCompress = this.getParamOutputCompress(paraMap);// 获取目标文件是否压缩
		String pTargetCompressCodec = this.getParamOutputCompressCodec(paraMap);// 获取目标文件压缩方式
		String pOutputFileFieldSplitChars = this.getParamOutputFileFieldSplitChars(paraMap);// 获取目标文件列分隔符
		String pOutputFileRowsSplitChars = this.getParamOutputFileRowsSplitChars(paraMap);// 获取目标文件行分隔符
		int pNumMapTasks = this.getParamNumMapTasks(paraMap);// 获取task数量
		int pNumReduceTasks = this.getParamNumReduceTasks(paraMap);// 获取reduce数量(map直接输出，reduce数量必须为0)
		String ptargetFilePath = this.getParamOutputTargetFilePath(paraMap);// 获取MR最终存放结果的目录

		// 打印解析后的输出参数参数
		this.printParameter(paraMap);

		try {
			Configuration conf = new Configuration();
			String[] dirInputs = pDirInput.split(",");
			Path inPaths[] = new Path[dirInputs.length];
			for (int i = 0; i < dirInputs.length; i++) {
				inPaths[i] = new Path(dirInputs[i]);
			}
			FileSystem fs = FileSystem.get(conf);
			Path outPath = new Path(pDirOutput);
			if (fs.exists(outPath)) {
				fs.delete(outPath, true);
			}

			for (int i = 0; i < pResource.length; i++) {
				conf.addResource(pResource[i]);
			}
			Job job = Job.getInstance(conf);
			conf = job.getConfiguration();
			job.setJarByClass(MRHiveToFileService.class);

			job.setJobName(pJobName);
			job.setPriority(pJobPriority);
			conf.set(MRConfiguration.FS_DEFAULT_NAME, pHdfsBaseUrl);
			conf.set(MRConfiguration.MAPRED_JOB_TRACKER, pJobTrackerHost);

			if (pIsFilter) {
				FileInputFormat.setInputPathFilter(job, RegexPathFilter.class);
				conf.set(FileConfiguration.INPUT_FILE_REGEXPATHFILTER, pRegFilterFile);
				conf.setBoolean(FileConfiguration.INPUT_FILE_REGEXPATHFILTER_TYPE, pRegFilterType);
			}

			FileInputFormat.setInputPaths(job, inPaths);
			FileOutputFormat.setOutputPath(job, outPath);
			job.setMapperClass(DBMapper.class);
			job.setReducerClass(DBReducer.class);

			// input from db
			job.setMapOutputKeyClass(DBRecord.class);
			job.setMapOutputValueClass(DBRecord.class);

			HiveConfiguration hiveConf = new HiveConfiguration(conf);
			hiveConf.setInputFieldNames(pInputFieldNames);// 设置输入字段顺序,与文件中顺序对应
			hiveConf.setRelatedGroupFieldMethod(pGroupFieldMethod);
			if (pIsCombinerClass) {
				job.setCombinerClass(DBGroupReducer.class);
			}

			// 数据驱动和查询sql
			hiveConf.configureInputHive(hiveDriverClass, pInputHiveDBUrl, pInputHiveDBUserName, pInputHiveDBPasswd);
			HiveInputFormat.setInput(job, DBRecord.class, pInputQuery);

			// output to file
			job.setOutputFormatClass(TextOutputFormat.class);// 输出到文本文件，保留原有压缩设置
			TextOutputFormat.setOutputParameter(job, pTargetCompress, pTargetCompressCodec, pOutputFileFieldSplitChars,
					pOutputFileRowsSplitChars);
			hiveConf.setOutputFieldNames(pTargetFieldNames);

			new MRConfiguration(conf).setNumMapTasks(pNumMapTasks);
			job.setNumReduceTasks(pNumReduceTasks);
			job.setOutputKeyClass(DBRecord.class);
			job.setOutputValueClass(NullWritable.class);
			job.waitForCompletion(true);
			// JobClient.runJob(conf);

			// 将文件从临时目录拷贝到目标目录下
			HDFSUtils.mvFiles(conf, pDirOutput + "/part-*", ptargetFilePath);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取Hive查询sql语句
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	private String getParamInputQuery(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("inputQuery");
		if (null == para || para.trim().length() <= 0) {
			String meg = "Hive查询sql语句<inputQuery>不能为空.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		return para;
	}

	/**
	 * hive数据库密码
	 * 
	 * @param paraMap
	 * @return
	 */
	private String getParamInputHiveDBPasswd(Map<String, String> paraMap) {
		String para = paraMap.get("inputHiveDBPasswd");
		if (null == para || para.trim().length() <= 0) {
			MRLog.warn(LOG, "hive数据库密码<hiveDBPasswd>为空.");
			return "";
		}

		return para;
	}

	/**
	 * 获取hive数据库用户名
	 * 
	 * @param paraMap
	 * @return
	 */
	private String getParamInputHiveDBUserName(Map<String, String> paraMap) {
		String para = paraMap.get("inputHiveDBUserName");
		if (null == para || para.trim().length() <= 0) {
			MRLog.warn(LOG, "hive数据库用户名<hiveDBUserName>为空.");
			return "";
		}

		return para;
	}

	/**
	 * 获取hive数据地址
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	private String getParamInputHiveDBUrl(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("inputHiveDBUrl");
		if (null == para || para.trim().length() <= 0) {
			String meg = "Hive数据库地址<hiveDBUrl>不能为空.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}
		return para;
	}

	/**
	 * 业务场景一：map直接输出
	 * 
	 * @return
	 */
	public static Map<String, String> testNoReduceParamter() {
		// 系统参数
		String resource = "hbase-default.xml,hbase-site.xml";// 以逗号隔开
		String jobName = "M-R Hive to File(noreduce)";
		String jobPriority = "HIGH";
		String hdfsBaseUrl = "hdfs://hadoop01:9000";
		String jobTrackerHost = "hadoop01:9001";
		String dirInput = "/wanghao/isomerism";
		String dirOutput = "/wanghao/isomerism-result1";
		String regFilterFile = "mrdata.txt";// 过滤正则表达式
		String isFilter = "false"; // 是否需要过滤，true表示启用过滤，否则不启用过滤
		String regFilterType = "false";
		String isinputMapEnd = "true"; // 需要map直接输出

		// 输入
		String inputHiveDBUrl = "jdbc:hive://133.37.251.207:10000/default";
		String inputHiveDBUserName = "";
		String inputHiveDBPasswd = "";
		String inputQuery = "select sid, snumber, rowcounts from t_mrtest_no"; // 不能有分号
		String srcFieldNames = "sid,snumber,rowcounts"; // 输入字段

		// 输出
		String targetFilePath = "/wanghao/isomerism-hiveToFile";
		String targetFieldNames = "id,number,message";
		String outputFileFieldSplitChars = "^*";
		String outputFileRowsSplitChars = "\n\n";
		String targetCompress = "false";
		String targetCompressCodec = HDFSUtils.CompressCodec.SnappyCodec.toString();

		// 任务数
		String numMapTasks = "2";
		String numReduceTasks = "0";

		// 输入输出关系
		String targetToSrcField = "id:sid,number:snumber,message:rowcounts";// 目标字段:源字段

		Map<String, String> paraMap = new HashMap<String, String>();
		// 设置系统配置
		paraMap.put("resource", resource);
		paraMap.put("jobName", jobName);
		paraMap.put("jobPriority", jobPriority);
		paraMap.put("hdfsBaseUrl", hdfsBaseUrl);
		paraMap.put("jobTrackerHost", jobTrackerHost);
		paraMap.put("dirInput", dirInput);
		paraMap.put("dirOutput", dirOutput);
		paraMap.put("isFilter", isFilter);
		paraMap.put("regFilterFile", regFilterFile);
		paraMap.put("regFilterType", regFilterType);
		paraMap.put("isinputMapEnd", isinputMapEnd);

		// 设置输入
		paraMap.put("inputHiveDBUrl", inputHiveDBUrl);
		paraMap.put("inputHiveDBUserName", inputHiveDBUserName);
		paraMap.put("inputHiveDBPasswd", inputHiveDBPasswd);
		paraMap.put("inputQuery", inputQuery);
		paraMap.put("srcFieldNames", srcFieldNames);

		// 设置输出
		paraMap.put("targetFieldNames", targetFieldNames);
		paraMap.put("outputFileFieldSplitChars", outputFileFieldSplitChars);
		paraMap.put("outputFileRowsSplitChars", outputFileRowsSplitChars);
		paraMap.put("targetCompress", targetCompress);
		paraMap.put("targetCompressCodec", targetCompressCodec);
		paraMap.put("targetFilePath", targetFilePath);

		// 设置任务数
		paraMap.put("numMapTasks", numMapTasks);
		paraMap.put("numReduceTasks", numReduceTasks);

		// 设置输入与输出关系
		paraMap.put("targetToSrcField", targetToSrcField);

		return paraMap;
	}

	/**
	 * 业务场景一：mr
	 * 
	 * @return
	 */
	public static Map<String, String> testMRParamter() {
		// 系统参数
		String resource = "hbase-default.xml,hbase-site.xml";// 以逗号隔开
		String jobName = "M-R Hive to File";
		String jobPriority = "HIGH";
		String hdfsBaseUrl = "hdfs://hadoop01:9000";
		String jobTrackerHost = "hadoop01:9001";
		String dirInput = "/wanghao/isomerism";
		String dirOutput = "/wanghao/isomerism-result1";
		String regFilterFile = "mrdata.txt";// 过滤正则表达式
		String isFilter = "false"; // 是否需要过滤，true表示启用过滤，否则不启用过滤
		String regFilterType = "false";
		String isinputMapEnd = "false"; // 需要map直接输出

		// 输入
		String inputHiveDBUrl = "jdbc:hive://133.37.251.207:10000/default";
		String inputHiveDBUserName = "";
		String inputHiveDBPasswd = "";
		String inputQuery = "select sid, sprice, rowcounts from t_mrtest_no"; // 不能有分号
		String srcFieldNames = "sid,sprice,rowcounts"; // 输入字段
		String isCombinerClass = "false"; // map是否合

		// 输出
		String targetFilePath = "/wanghao/isomerism-hiveToFile";
		String targetFieldNames = "id,minprice,maxprice,message";
		String outputFileFieldSplitChars = "^";
		String outputFileRowsSplitChars = "\n";
		String targetCompress = "true";
		String targetCompressCodec = HDFSUtils.CompressCodec.GzipCodec.toString();

		// 任务数
		String numMapTasks = "2";
		String numReduceTasks = "1";

		// 目标字段(targFieldNames):源字段(srcFieldNames):统计源字段的方法
		String groupFieldMethod = "id:sid:NONE,minprice:sprice:MIN,maxprice:sprice:MAX,message:rowcounts:NONE";

		Map<String, String> paraMap = new HashMap<String, String>();
		// 设置系统配置
		paraMap.put("resource", resource);
		paraMap.put("jobName", jobName);
		paraMap.put("jobPriority", jobPriority);
		paraMap.put("hdfsBaseUrl", hdfsBaseUrl);
		paraMap.put("jobTrackerHost", jobTrackerHost);
		paraMap.put("dirInput", dirInput);
		paraMap.put("dirOutput", dirOutput);
		paraMap.put("isFilter", isFilter);
		paraMap.put("regFilterFile", regFilterFile);
		paraMap.put("regFilterType", regFilterType);
		paraMap.put("isinputMapEnd", isinputMapEnd);

		// 设置输入
		paraMap.put("inputHiveDBUrl", inputHiveDBUrl);
		paraMap.put("inputHiveDBUserName", inputHiveDBUserName);
		paraMap.put("inputHiveDBPasswd", inputHiveDBPasswd);
		paraMap.put("inputQuery", inputQuery);
		paraMap.put("srcFieldNames", srcFieldNames);
		paraMap.put("isCombinerClass", isCombinerClass);

		// 设置输出
		paraMap.put("targetFieldNames", targetFieldNames);
		paraMap.put("outputFileFieldSplitChars", outputFileFieldSplitChars);
		paraMap.put("outputFileRowsSplitChars", outputFileRowsSplitChars);
		paraMap.put("targetCompress", targetCompress);
		paraMap.put("targetCompressCodec", targetCompressCodec);
		paraMap.put("targetFilePath", targetFilePath);

		// 设置任务数
		paraMap.put("numMapTasks", numMapTasks);
		paraMap.put("numReduceTasks", numReduceTasks);

		// 设置输入与输出关系
		paraMap.put("groupFieldMethod", groupFieldMethod);

		return paraMap;
	}

	/**
	 * 测试入口函数
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		MRHiveToFileService s = new MRHiveToFileService();
		// s.run(testMRParamter());
		s.run(testNoReduceParamter());
	}
}