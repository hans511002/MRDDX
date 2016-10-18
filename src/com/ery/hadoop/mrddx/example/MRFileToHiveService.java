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
import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.DBReducer;
import com.ery.hadoop.mrddx.FileMapper;
import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.file.FileConfiguration;
import com.ery.hadoop.mrddx.file.RegexPathFilter;
import com.ery.hadoop.mrddx.file.TextInputFormat;
import com.ery.hadoop.mrddx.hive.HiveConfiguration;
import com.ery.hadoop.mrddx.hive.HiveOutputFormat;
import com.ery.hadoop.mrddx.util.HDFSUtils;

/**
 * 从文件到Hive的MRService Copyrights @ 2012,Tianyuan DIC Information Co.,Ltd. All
 * rights reserved.
 * 
 * @author wanghao
 * @version v1.0
 */
public class MRFileToHiveService extends MRTestJOBService {
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

		String pOutHiveDBUrl = this.getParamOutHiveDBUrl(paraMap);// 获取Hive数据库地址
		String pOutHiveDBUserName = this.getParamOutHiveDBUserName(paraMap);// 获取Hive数据库用户名
		String pOutHiveDBPasswd = this.getParamOutHiveDBPasswd(paraMap);// 获取Hive数据库密码

		// 输入参数
		String pInputFieldNames = this.getParamInputFieldNames(paraMap);// 获取源字段
		String pInputFileFieldSplitChars = this.getParamInputFileFieldSplitChars(paraMap);// 获取源文件列分隔符
		String pInputFileRowsSplitChars = this.getParamInputFileRowsSplitChars(paraMap);// 获取源文件行分隔符
		String pGroupFieldMethod = this.getParamRelatedGroupFieldMethod(paraMap);// 获取目标字段:源字段:统计方法
		boolean pIsCombinerClass = this.getParamInputIsCombinerClass(paraMap);// 获取是否在map中合并

		// 输出参数
		String pTargetTableName = this.getParamTargetTableName(paraMap);// 获取目标表名
		String pTargetFieldNames[] = this.getParamOutputFieldNames(paraMap);// 获取目标字段
		boolean pTargetCompress = this.getParamOutputCompress(paraMap);// 获取目标文件是否压缩
		String pTargetCompressCodec = this.getParamOutputCompressCodec(paraMap);// 获取目标文件压缩方式
		String pTargetDDLHQL = this.getParamTargetDDLHQL(paraMap);// 获取输出需要执行的ddl
		// sql
		String pOutputFileFieldSplitChars = this.getParamOutputFileFieldSplitChars(paraMap);// 获取目标文件列分隔符
		String pOutputFileRowsSplitChars = this.getParamOutputFileRowsSplitChars(paraMap);// 获取目标文件行分隔符

		int pNumMapTasks = this.getParamNumMapTasks(paraMap);// 获取task数量
		int pNumReduceTasks = this.getParamNumReduceTasks(paraMap);// 获取reduce数量(map直接输出，reduce数量必须为0)
		String ptargetFilePath = this.getParamOutputTargetFilePath(paraMap);// 获取MR最终存放结果的目录

		// 打印解析后的输出参数参数
		printParameter(paraMap);

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
			job.setJarByClass(MRFileToHiveService.class);

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
			job.setMapperClass(FileMapper.class);
			job.setReducerClass(DBReducer.class);

			// input from file
			job.setInputFormatClass(TextInputFormat.class);
			job.setMapOutputKeyClass(DBRecord.class);
			job.setMapOutputValueClass(DBRecord.class);

			FileConfiguration fileConf = new FileConfiguration(conf);
			fileConf.setInputFieldNames(pInputFieldNames);// 设置输入字段顺序,与文件中顺序对应
			fileConf.setInputFileFieldSplitChars(pInputFileFieldSplitChars);
			fileConf.setInputFileRowsSplitChars(pInputFileRowsSplitChars);
			fileConf.setRelatedGroupFieldMethod(pGroupFieldMethod);
			if (pIsCombinerClass) {
				job.setCombinerClass(DBGroupReducer.class);
			}

			// output to db
			HiveConfiguration hiveConf = new HiveConfiguration(conf);
			hiveConf.configureOutputHive(conf, hiveDriverClass, pOutHiveDBUrl, pOutHiveDBUserName, pOutHiveDBPasswd);
			// 设置输出系统参数
			HiveOutputFormat.setOutput(job, pTargetTableName);
			// 设置输出的数据参数
			HiveOutputFormat.setOutputParameter(conf, pTargetCompress, pTargetCompressCodec,
					pOutputFileFieldSplitChars, pOutputFileRowsSplitChars, pTargetDDLHQL);
			// 设置输出的字段
			fileConf.setOutputFieldNames(pTargetFieldNames);

			new MRConfiguration(conf).setNumMapTasks(pNumMapTasks);
			job.setNumReduceTasks(pNumReduceTasks);
			job.setOutputKeyClass(DBRecord.class);
			job.setOutputValueClass(NullWritable.class);
			job.waitForCompletion(true);
			// JobClient.runJob(conf);

			// 将文件从临时目录拷贝到目标目录下
			if (null != ptargetFilePath) {
				HDFSUtils.mvFiles(conf, pDirOutput + "/part-*", ptargetFilePath);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 对hive数据库执行的DDLHQL
	 * 
	 * @param paraMap
	 * @return
	 */
	private String getParamTargetDDLHQL(Map<String, String> paraMap) {
		String para = paraMap.get("targetDDLHQL");
		if (null == para || para.trim().length() <= 0) {
			String meg = "[MR ERROR]对hive数据库执行的DDLHQL<targetDDLHQL>未设置.";
			LOG.error(meg);
			return "";
		}

		return para;
	}

	/**
	 * 获取hive目标表名
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	private String getParamTargetTableName(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("targetTableName");
		if (null == para || para.trim().length() <= 0) {
			String meg = "[MR ERROR]Hive表名<targetTableName>不能为空.";
			LOG.error(meg);
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
	private String getParamOutHiveDBPasswd(Map<String, String> paraMap) {
		String para = paraMap.get("outHiveDBPasswd");
		if (null == para || para.trim().length() <= 0) {
			LOG.warn("[MR WARN]hive数据库密码<hiveDBPasswd>为空.");
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
	private String getParamOutHiveDBUserName(Map<String, String> paraMap) {
		String para = paraMap.get("outHiveDBUserName");
		if (null == para || para.trim().length() <= 0) {
			LOG.warn("[MR WARN]hive数据库用户名<hiveDBUserName>为空.");
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
	private String getParamOutHiveDBUrl(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("outHiveDBUrl");
		if (null == para || para.trim().length() <= 0) {
			String meg = "[MR ERROR]Hive数据库地址<outHiveDBUrl>不能为空.";
			LOG.error(meg);
			throw new Exception(meg);
		}
		return para;
	}

	/**
	 * 例如：业务场景一
	 * 
	 * @return
	 */
	public static Map<String, String> testNoReduceParamter() {
		// 系统参数
		String resource = "hbase-default.xml,hbase-site.xml";// 以逗号隔开
		String jobName = "M-R File to Hive";
		String jobPriority = "HIGH";
		String hdfsBaseUrl = "hdfs://hadoop01:9000";
		String jobTrackerHost = "hadoop01:9001";
		String dirInput = "/wanghao/isomerism";
		String dirOutput = "/wanghao/isomerism-hive";
		String regFilterFile = "mrdata.txt";// 过滤正则表达式
		String isFilter = "false"; // 是否需要过滤，true表示启用过滤，否则不启用过滤
		String regFilterType = "false";
		String isinputMapEnd = "true"; // 需要map直接输出

		// 输入参数
		String inputFieldNames = "time,id,type,price,number,message"; // 输入字段
		String inputFileFieldSplitChars = "\\^";
		String inputFileRowsSplitChars = "\n";
		String isCombinerClass = "false"; // map是否合

		// 输出配置
		String outHiveDBUrl = "jdbc:hive://133.37.251.207:10000/default";
		String outHiveDBUserName = "";
		String outHiveDBPasswd = "";
		String targetTableName = "t_mrtest_no";
		String targetFieldNames = "sid,sprice,snumber,smessage"; // 输出字段
		String outputFileFieldSplitChars = "^";
		String outputFileRowsSplitChars = "\n";
		String targetCompress = "true";
		String targetCompressCodec = HDFSUtils.CompressCodec.GzipCodec.toString();
		String targetDDLHQL = "CREATE EXTERNAL TABLE IF NOT EXISTS t_mrtest_no"
				+ " (sid bigint, sprice double, snumber int, smessage string)"
				+ " ROW FORMAT DELIMITED FIELDS TERMINATED BY '^'" + " STORED AS TEXTFILE"
				+ " LOCATION 'hdfs://hadoop01:9000/hive90/warehouse/t_mrtest_no';";// ddlHQL语句（创建表或者修改表，可以多条，按分号隔开）
		String targetFilePath = "/hive90/warehouse/" + targetTableName;

		// 任务数
		String numMapTasks = "2";
		String numReduceTasks = "0";

		// 输入输出关系
		String targetToSrcField = "sid:id,sprice:price,snumber:number,smessage:message";// 目标字段:源字段

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
		paraMap.put("inputFieldNames", inputFieldNames);
		paraMap.put("inputFileFieldSplitChars", inputFileFieldSplitChars);
		paraMap.put("inputFileRowsSplitChars", inputFileRowsSplitChars);
		paraMap.put("isCombinerClass", isCombinerClass);

		// 设置输出
		paraMap.put("outHiveDBUrl", outHiveDBUrl);
		paraMap.put("outHiveDBUserName", outHiveDBUserName);
		paraMap.put("outHiveDBPasswd", outHiveDBPasswd);
		paraMap.put("targetTableName", targetTableName);
		paraMap.put("targetFieldNames", targetFieldNames);
		paraMap.put("outputFileFieldSplitChars", outputFileFieldSplitChars);
		paraMap.put("outputFileRowsSplitChars", outputFileRowsSplitChars);
		paraMap.put("targetCompress", targetCompress);
		paraMap.put("targetCompressCodec", targetCompressCodec);
		paraMap.put("targetDDLHQL", targetDDLHQL);
		paraMap.put("targetFilePath", targetFilePath);

		// 设置任务数
		paraMap.put("numMapTasks", numMapTasks);
		paraMap.put("numReduceTasks", numReduceTasks);

		// 设置输入与输出关系
		paraMap.put("targetToSrcField", targetToSrcField);

		return paraMap;
	}

	/**
	 * 例如：业务场景二
	 * 
	 * @return
	 */
	public static Map<String, String> testMRParamter() {
		// 系统参数
		String resource = "hbase-default.xml,hbase-site.xml";// 以逗号隔开
		String jobName = "M-R File to Hive";
		String jobPriority = "HIGH";
		String hdfsBaseUrl = "hdfs://hadoop01:9000";
		String jobTrackerHost = "hadoop01:9001";
		String dirInput = "/wanghao/isomerism";
		String dirOutput = "/wanghao/isomerism-hive";
		String regFilterFile = "mrdata.txt";// 过滤正则表达式
		String isFilter = "true"; // 是否需要过滤，true表示启用过滤，否则不启用过滤
		String regFilterType = "true";

		// 输入参数
		String inputFieldNames = "time,id,type,price,number,message"; // 输入字段
		String inputFileFieldSplitChars = "\\^";
		String inputFileRowsSplitChars = "\n";
		String isCombinerClass = "false"; // map是否合

		// 输出配置
		String outHiveDBUrl = "jdbc:hive://133.37.251.207:10000/default";
		String outHiveDBUserName = "";
		String outHiveDBPasswd = "";
		String targetTableName = "t_mrtest_seq";
		String targetFieldNames = "sid,sprice,mprice,avgnumber,sumnumber,rowcounts"; // 输出字段
		String outputFileFieldSplitChars = "^";
		String outputFileRowsSplitChars = "\n";
		String targetCompress = "false";
		String targetCompressCodec = HDFSUtils.CompressCodec.GzipCodec.toString();
		String targetDDLHQL = "CREATE EXTERNAL TABLE IF NOT EXISTS t_mrtest_rc"
				+ " (sid bigint, sprice double, mprice double, avgnumber double, sumnumber double, rowcounts bigint)"
				+ " ROW FORMAT DELIMITED FIELDS TERMINATED BY '^'" + " STORED AS RCFILE"
				+ " LOCATION 'hdfs://hadoop01:9000/hive90/warehouse/t_mrtest_rc';";// ddlHQL语句（创建表或者修改表，可以多条，按顺序执行）
		String targetFilePath = "/hive90/warehouse/" + targetTableName;

		// 任务数
		String numMapTasks = "2";
		String numReduceTasks = "1";

		// 输入输出关系
		String groupFieldMethod = "sid:id:NONE,sprice:price:MIN,mprice:price:MAX,avgnumber:number:AVG,sumnumber:number:SUM,rowcounts:id:COUNT";

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

		// 设置输入
		paraMap.put("inputFieldNames", inputFieldNames);
		paraMap.put("inputFileFieldSplitChars", inputFileFieldSplitChars);
		paraMap.put("inputFileRowsSplitChars", inputFileRowsSplitChars);
		paraMap.put("isCombinerClass", isCombinerClass);

		// 设置输出
		paraMap.put("outHiveDBUrl", outHiveDBUrl);
		paraMap.put("outHiveDBUserName", outHiveDBUserName);
		paraMap.put("outHiveDBPasswd", outHiveDBPasswd);
		paraMap.put("targetTableName", targetTableName);
		paraMap.put("targetFieldNames", targetFieldNames);
		paraMap.put("outputFileFieldSplitChars", outputFileFieldSplitChars);
		paraMap.put("outputFileRowsSplitChars", outputFileRowsSplitChars);
		paraMap.put("targetCompress", targetCompress);
		paraMap.put("targetCompressCodec", targetCompressCodec);
		paraMap.put("targetDDLHQL", targetDDLHQL);
		paraMap.put("targetFilePath", targetFilePath);

		// 设置任务数
		paraMap.put("numMapTasks", numMapTasks);
		paraMap.put("numReduceTasks", numReduceTasks);

		// 设置输入与输出关系
		paraMap.put("groupFieldMethod", groupFieldMethod);
		return paraMap;
	}

	public static void main(String[] args) throws Exception {
		MRFileToHiveService s = new MRFileToHiveService();
		s.run(testMRParamter()); // 测试reduce输出
		// s.run(testNoReduceParamter());// 测试map直接输出
	}
}