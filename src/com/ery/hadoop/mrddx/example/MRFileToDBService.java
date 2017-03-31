package com.ery.hadoop.mrddx.example;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.ery.hadoop.mrddx.DBGroupReducer;
import com.ery.hadoop.mrddx.DBMapper;
import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.DBReducer;
import com.ery.hadoop.mrddx.FileMapper;
import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.RCFileMapper;
import com.ery.hadoop.mrddx.db.DBConfiguration;
import com.ery.hadoop.mrddx.db.DBOutputFormat;
import com.ery.hadoop.mrddx.file.FileConfiguration;
import com.ery.hadoop.mrddx.file.RegexPathFilter;
import com.ery.hadoop.mrddx.file.TextInputFormat;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.HDFSUtils.FileType;

/**

 * 

 * @description
 * @date 2012-11-15
 */
public class MRFileToDBService extends MRTestJOBService {
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
		boolean pIsinputMapEnd = this.getParamSysIsMapEnd(paraMap);// 获取是否map直接输出

		// 输入参数
		String pInputFieldNames = this.getParamInputFieldNames(paraMap);// 获取源字段
		String pInputFileFieldSplitChars = this.getParamInputFileFieldSplitChars(paraMap);// 获取源文件列分隔符
		String pInputFileRowsSplitChars = this.getParamInputFileRowsSplitChars(paraMap);// 获取源文件行分隔符
		String pInputFileType = this.getParamInputFileType(paraMap);// 获取文件类型
		String pGroupFieldMethod = this.getParamRelatedGroupFieldMethod(paraMap);// 获取目标字段:源字段:统计方法
		boolean pIsCombinerClass = this.getParamInputIsCombinerClass(paraMap);// 获取是否在map中合并

		// 输出参数
		String pTargetTableName = this.getParamTargetTableName(paraMap);// 目标表名
		String pTargetFieldNames[] = this.getParamOutputFieldNames(paraMap);// 目标字段
		String pDBDriverClass = this.getParamDBDriverClass(paraMap);// 数据库驱动
		String pDBUrl = this.getParamDBUrl(paraMap);// 数据地址
		String pDBUserName = this.getParamDBUserName(paraMap);// 数据库用户名
		String pDBPasswd = this.getParamDBPasswd(paraMap);// 数据库

		int pNumMapTasks = this.getParamNumMapTasks(paraMap);// 获取task数量
		int pNumReduceTasks = this.getParamNumReduceTasks(paraMap);// 获取reduce数量(map直接输出，reduce数量必须为0)

		// 打印解析后的输出参数参数
		printParameter(paraMap);

		try {
			Configuration conf = new JobConf(MRFileToDBService.class);
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
			conf.set(MRConfiguration.FS_DEFAULT_NAME, pHdfsBaseUrl);
			conf.set(MRConfiguration.MAPRED_JOB_TRACKER, pJobTrackerHost);

			Job job = Job.getInstance(conf);
			conf = job.getConfiguration();
			job.setJarByClass(MRFileToDBService.class);
			job.setJobName(pJobName);
			job.setPriority(pJobPriority);
			// conf.setJobPriority(pJobPriority);
			// conf.set(JobContext.PRIORITY, pJobPriority.toString());
			if (pIsFilter) {
				FileInputFormat.setInputPathFilter(job, RegexPathFilter.class);
				conf.set(FileConfiguration.INPUT_FILE_REGEXPATHFILTER, pRegFilterFile);
				conf.setBoolean(FileConfiguration.INPUT_FILE_REGEXPATHFILTER_TYPE, pRegFilterType);
			}

			FileInputFormat.setInputPaths(job, inPaths);
			FileOutputFormat.setOutputPath(job, outPath);
			if (FileType.RCFILE.name().equals(pInputFileType)) {
				job.setMapperClass(RCFileMapper.class);
			} else if (FileType.TEXTFILE.name().equals(pInputFileType)
					|| FileType.SEQUENCEFILE.name().equals(pInputFileType)) {
				job.setMapperClass(FileMapper.class);
			} else {
				job.setMapperClass(DBMapper.class);
			}
			job.setReducerClass(DBReducer.class);

			// 输入参数
			job.setInputFormatClass(TextInputFormat.class);
			job.setMapOutputKeyClass(DBRecord.class);
			job.setMapOutputValueClass(DBRecord.class);

			FileConfiguration dbconf = new FileConfiguration(conf);
			dbconf.setInputMapEnd(pIsinputMapEnd);
			dbconf.setInputFieldNames(pInputFieldNames);// 设置输入字段顺序,与文件中顺序对应
			dbconf.setInputFileFieldSplitChars(pInputFileFieldSplitChars);
			dbconf.setInputFileRowsSplitChars(pInputFileRowsSplitChars);
			dbconf.setRelatedGroupFieldMethod(pGroupFieldMethod);
			if (pIsCombinerClass) {
				job.setCombinerClass(DBGroupReducer.class);
				job.setCombinerKeyGroupingComparatorClass(DBRecord.class);
			}

			// 输出配置
			DBConfiguration.configureOutputDB(conf, pDBDriverClass, pDBUrl, pDBUserName, pDBPasswd);
			// targetFieldNames可为空，将使用输入查询的字段名 但是不会自动新建数据表
			DBOutputFormat.setOutput(job, pTargetTableName, pTargetFieldNames);

			new MRConfiguration(conf).setNumMapTasks(pNumMapTasks);
			job.setNumReduceTasks(pNumReduceTasks);
			job.setOutputKeyClass(DBRecord.class);
			job.setOutputValueClass(NullWritable.class);

			job.waitForCompletion(true);
			// JobClient.runJob(conf);
		} catch (Exception e) {

		}
	}

	/**
	 * 目标表名
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	private String getParamTargetTableName(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("targetTableName");
		if (null == para || para.length() <= 0) {
			String meg = "目标表名<targetTableName>未设置.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		return para;
	}

	/**
	 * 获取目标字段
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	protected String[] getParamOutputFieldNames(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("targetFieldNames");
		if (null == para || para.trim().length() <= 0) {
			MRLog.warn(LOG, "目标字段<targetFieldNames>未设置，默认为表所有字段");
			return new String[0];
		}

		return para.split(","); // 输出字段
	}

	/**
	 * 获取数据库驱动
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	private String getParamDBDriverClass(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("dbDriverClass");
		if (null == para || para.length() <= 0) {
			String meg = "数据库驱动<dbDriverClass>未设置.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		return para;
	}

	/**
	 * 获取数据库地址
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	private String getParamDBUrl(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("dbUrl");
		if (null == para || para.length() <= 0) {
			String meg = "数据库地址<dbUrl>未设置.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		return para;
	}

	/**
	 * 获取数据库用户名
	 * 
	 * @param paraMap
	 * @return
	 */
	private String getParamDBUserName(Map<String, String> paraMap) {
		String para = paraMap.get("dbUserName");
		if (null == para || para.length() <= 0) {
			MRLog.warn(LOG, "数据库用户名<dbUserName>未设置.默认为空");
		}

		return para;
	}

	/**
	 * 获取数据库密码
	 * 
	 * @param paraMap
	 * @return
	 */
	private String getParamDBPasswd(Map<String, String> paraMap) {
		String para = paraMap.get("dbPasswd");
		if (null == para || para.length() <= 0) {
			MRLog.warn(LOG, "数据库密码<dbUserName>未设置.默认为空");
		}

		return para;
	}

	protected static Map<String, String> testNoReduceParamter() {
		// 系统参数
		String resource = "hbase-default.xml,hbase-site.xml";// 以逗号隔开
		String jobName = "M-R File to msqldb(noreduce)";
		String jobPriority = "HIGH";
		String hdfsBaseUrl = "hdfs://hadoop01:9000";
		String jobTrackerHost = "hadoop01:9001";
		String dirInput = "/wanghao/isomerism";
		String dirOutput = "/wanghao/isomerism-hive";
		String regFilterFile = "mrdata.txt";// 过滤正则表达式
		String isFilter = "true"; // 是否需要过滤，true表示启用过滤，否则不启用过滤
		String regFilterType = "true";
		String isinputMapEnd = "true"; // 需要map直接输出

		// 输入参数
		String inputFieldNames = "time,id,type,price,number,message"; // 输入字段
		String inputFileFieldSplitChars = "[\\^;]";
		String inputFileRowsSplitChars = "\n";
		String inputFileType = "TEXTFILE";// 文件类型
		String isCombinerClass = "false"; // map是否合

		// 输出配置
		// mysql
		String dbDriverClass = "org.gjt.mm.mysql.Driver";
		String dbUrl = "jdbc:mysql://133.37.251.207:3306/mrtest";
		String dbUserName = "hive";
		String dbPasswd = "hive";
		// oracle
		// String dbDriverClass = "oracle.jdbc.driver.OracleDriver";
		// String dbUrl = "jdbc:oracle:thin:@133.37.251.241:1521:ora10";
		// String dbUserName = "meta";
		// String dbPasswd = "meta";
		String targetTableName = "t_mrtestnoreduceresult";
		String targetFieldNames = "sid,sprice,snumber,smessage"; // 输出字段（可以不设置，默认表的所有字段）
		String targetDDLSQL = ""; // ddlSQL语句（创建表或者修改表，可以多条，按顺序执行）

		// 任务数
		String numMapTasks = "2";
		String numReduceTasks = "0";

		// 输入输出关系
		String targetToSrcField = "sid:id,sprice:price,snumber:number,smessage:message";// 目标字段:源字段

		Map<String, String> paraMap = new HashMap<String, String>();
		// 设置系统配置
		paraMap.put("sysResource", resource);
		paraMap.put("sysJobName", jobName);
		paraMap.put("sysJobPriority", jobPriority);
		paraMap.put("sysHdfsBaseUrl", hdfsBaseUrl);
		paraMap.put("sysJobTrackerHost", jobTrackerHost);
		paraMap.put("sysDirInput", dirInput);
		paraMap.put("sysDirOutput", dirOutput);
		paraMap.put("sysIsFilter", isFilter);
		paraMap.put("sysRegFilterFile", regFilterFile);
		paraMap.put("sysRegFilterType", regFilterType);
		paraMap.put("sysIsMapEnd", isinputMapEnd);

		// 设置输入
		paraMap.put("inputFieldNames", inputFieldNames);
		paraMap.put("inputFileFieldSplitChars", inputFileFieldSplitChars);
		paraMap.put("inputFileRowsSplitChars", inputFileRowsSplitChars);
		paraMap.put("inputFileType", inputFileType);
		paraMap.put("inputIsCombinerClass", isCombinerClass);

		// 设置输出
		paraMap.put("dbDriverClass", dbDriverClass);
		paraMap.put("dbUrl", dbUrl);
		paraMap.put("dbUserName", dbUserName);
		paraMap.put("dbPasswd", dbPasswd);
		paraMap.put("targetTableName", targetTableName);
		paraMap.put("targetFieldNames", targetFieldNames);
		paraMap.put("targetDDLHQL", targetDDLSQL);

		// 设置任务数
		paraMap.put("numMapTasks", numMapTasks);
		paraMap.put("numReduceTasks", numReduceTasks);

		// 设置输入与输出关系
		paraMap.put("relatedInToOutField", targetToSrcField);
		return paraMap;
	}

	protected static Map<String, String> testMRParamter() {
		// 系统参数
		String sysResource = "hbase-default.xml,hbase-site.xml";// 以逗号隔开
		String sysJobName = "M-R File to msqldb";
		String sysJobPriority = "HIGH";
		String sysHdfsBaseUrl = "hdfs://hadoop01:9000";
		String sysJobTrackerHost = "hadoop01:9001";
		String sysDirInput = "/wanghao/isomerism-result";
		String sysDirOutput = "/wanghao/isomerism-hive";
		String sysIsFilter = "true"; // 是否需要过滤，true表示启用过滤，否则不启用过滤
		String sysRegFilterFile = "part-00000";// 过滤正则表达式
		String sysRegFilterType = "true";
		String sysIsinputMapEnd = "false"; // 需要map直接输出

		// 输入参数
		String inputFieldNames = "sid,sprice,mprice,avgnumber,sumnumber,rowcounts";// 测试seq文件的
		// String inputFieldNames = "time,id,type,price,number,message"; //
		// 输入字段(注：必须和文件中数据的顺序一致)
		String inputFileFieldSplitChars = "[\\^;]";
		String inputFileRowsSplitChars = "\n";
		String inputFileType = "RCFILE";// TEXTFILE,SEQUENCEFILE,RCFILE
		String inputIsCombinerClass = "false"; // map是否合

		// 输出配置
		// mysql
		String dbDriverClass = "org.gjt.mm.mysql.Driver";
		String dbUrl = "jdbc:mysql://133.37.251.207:3306/mrtest";
		String dbUserName = "hive";
		String dbPasswd = "hive";
		// oracle
		// String dbDriverClass = "oracle.jdbc.driver.OracleDriver";
		// String dbUrl = "jdbc:oracle:thin:@133.37.251.241:1521:ora10";
		// String dbUserName = "meta";
		// String dbPasswd = "meta";

		String targetTableName = "t_mrtestresult";
		String targetFieldNames = "sid,sprice,mprice,avgnumber,sumnumber,rowcounts"; // 输出字段（可以不设置，默认表的所有字段）
		String targetDDLSQL = "";// ddlSQL语句（创建表或者修改表，可以多条，按顺序执行）

		// 任务数
		String numMapTasks = "2";
		String numReduceTasks = "2";

		// 输入输出关系
		// String groupFieldMethod =
		// "sid:id:NONE,sprice:price:MIN,mprice:price:MAX," +
		// "avgnumber:number:AVG,sumnumber:number:SUM,rowcounts:id:COUNT";
		String groupFieldMethod = "sid:sid:NONE,sprice:sprice:MIN,mprice:mprice:MAX,"
				+ "avgnumber:avgnumber:AVG,sumnumber:sumnumber:SUM,rowcounts:rowcounts:NONE"; // 测试rc文件

		Map<String, String> paraMap = new HashMap<String, String>();
		// 设置系统配置
		paraMap.put("sysResource", sysResource);
		paraMap.put("sysJobName", sysJobName);
		paraMap.put("jobPriority", sysJobPriority);
		paraMap.put("hdfsBaseUrl", sysHdfsBaseUrl);
		paraMap.put("jobTrackerHost", sysJobTrackerHost);
		paraMap.put("dirInput", sysDirInput);
		paraMap.put("dirOutput", sysDirOutput);
		paraMap.put("sysIsFilter", sysIsFilter);
		paraMap.put("sysRegFilterFile", sysRegFilterFile);
		paraMap.put("sysRegFilterType", sysRegFilterType);
		paraMap.put("sysIsinputMapEnd", sysIsinputMapEnd);

		// 设置输入
		paraMap.put("inputFieldNames", inputFieldNames);
		paraMap.put("inputFileFieldSplitChars", inputFileFieldSplitChars);
		paraMap.put("inputFileRowsSplitChars", inputFileRowsSplitChars);
		paraMap.put("inputFileType", inputFileType);
		paraMap.put("inputIsCombinerClass", inputIsCombinerClass);

		// 设置输出
		paraMap.put("dbDriverClass", dbDriverClass);
		paraMap.put("dbUrl", dbUrl);
		paraMap.put("dbUserName", dbUserName);
		paraMap.put("dbPasswd", dbPasswd);
		paraMap.put("targetTableName", targetTableName);
		paraMap.put("targetFieldNames", targetFieldNames);
		paraMap.put("targetDDLHQL", targetDDLSQL);

		// 设置任务数
		paraMap.put("numMapTasks", numMapTasks);
		paraMap.put("numReduceTasks", numReduceTasks);

		// 设置输入与输出关系
		paraMap.put("groupFieldMethod", groupFieldMethod);

		return paraMap;
	}

	public static void main(String[] args) throws Exception {
		MRFileToDBService s = new MRFileToDBService();
		// s.run(testMRParamter());
		s.run(testNoReduceParamter());
	}
}