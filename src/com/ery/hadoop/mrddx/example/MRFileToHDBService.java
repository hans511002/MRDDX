package com.ery.hadoop.mrddx.example;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
import com.ery.hadoop.mrddx.hbase.HbaseConfiguration;
import com.ery.hadoop.mrddx.hbase.HbaseOutputFormat;

/**
 * 

 * 

 * @Comments

 * @version v1.0
 * @create Data 2012-12-31
 * 
 */
public class MRFileToHDBService extends MRTestJOBService {
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

		// 输入参数
		String pInputFieldNames = this.getParamInputFieldNames(paraMap);// 获取源字段
		String pInputFileFieldSplitChars = this.getParamInputFileFieldSplitChars(paraMap);// 获取源文件列分隔符
		String pInputFileRowsSplitChars = this.getParamInputFileRowsSplitChars(paraMap);// 获取源文件行分隔符
		String pGroupFieldMethod = this.getParamRelatedGroupFieldMethod(paraMap);// 获取目标字段:源字段:统计方法
		boolean pIsCombinerClass = this.getParamInputIsCombinerClass(paraMap);// 获取是否在map中合并

		// 输出参数
		String pTargetTableName = this.getParamTargetTableName(paraMap);// 获取HBASE表名
		String pTargetFamilyNames = this.getParamTargetFamilyNames(paraMap); // 获取列族列表
		String pTargetFieldNames[] = this.getParamOutputFieldNames(paraMap);// 获取目标字段
		String pRowKeyRule = this.getParamRowKeyRule(paraMap); // 获取rowkey规则
		int pHfileMaxfilesize = this.getParamHfileMaxfilesize(paraMap); // 获取HFile的最大值
		int pCompressionType = this.getParamCompressionType(paraMap); // 获取压缩类型
		int pCompactionCompressionType = this.getParamCompactionCompressionType(paraMap); // 获取合并压缩类型
		int pColmunBlocksize = this.getParamColmunBlocksize(paraMap); // 获取列块大小
		int pColmunMaxversion = this.getParamColmunMaxversion(paraMap); // 获取最大版本号
		int pColmunMinversion = this.getParamColmunMinversion(paraMap); // 获取最小版本号
		int pNumMapTasks = this.getParamNumMapTasks(paraMap);// 获取task数量
		int pNumReduceTasks = this.getParamNumReduceTasks(paraMap);// 获取reduce数量(map直接输出，reduce数量必须为0)

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
			job.setJarByClass(MRFileToHDBService.class);

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

			// 输入参数
			job.setInputFormatClass(TextInputFormat.class);
			job.setMapOutputKeyClass(DBRecord.class);
			job.setMapOutputValueClass(DBRecord.class);

			FileConfiguration fileConf = new FileConfiguration(conf);
			fileConf.setInputFieldNames(pInputFieldNames);
			fileConf.setInputFileFieldSplitChars(pInputFileFieldSplitChars);
			fileConf.setInputFileRowsSplitChars(pInputFileRowsSplitChars);
			fileConf.setRelatedGroupFieldMethod(pGroupFieldMethod);
			if (pIsCombinerClass) {
				job.setCombinerClass(DBGroupReducer.class);
			}

			// 输出配置
			fileConf.setOutputFieldNames(pTargetFieldNames);
			HbaseOutputFormat.setOutput(job, pTargetTableName, pRowKeyRule, 64 * 1024 * 1024, false, pHfileMaxfilesize,
					pCompressionType, pCompactionCompressionType, pColmunBlocksize, pColmunMaxversion,
					pColmunMinversion, HbaseConfiguration.BloomType_ROW, false, 20, pTargetFamilyNames);
			new MRConfiguration(conf).setNumMapTasks(pNumMapTasks);
			job.setNumReduceTasks(pNumReduceTasks);// 通常等于RegionServer数
			job.setOutputKeyClass(DBRecord.class);
			job.setOutputValueClass(NullWritable.class);
			job.waitForCompletion(true);
			// JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取输出表名
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	private String getParamTargetTableName(Map<String, String> paraMap) throws Exception {
		String param = paraMap.get("targetTableName");
		if (null == param || param.trim().length() <= 0) {
			String meg = "[MR ERROR]HBase输出表名<targetTableName>不能为空.";
			LOG.error(meg);
			throw new Exception(meg);
		}

		return param;
	}

	/**
	 * 获取行规则
	 * 
	 * @param paraMap
	 * @return
	 */
	private String getParamRowKeyRule(Map<String, String> paraMap) {
		String param = paraMap.get("rowKeyRule");
		if (null == param || param.trim().length() <= 0) {
			String rowkey = "tj-{DATE, yyyy-mm-dd}";
			LOG.warn("[MR WARN]表的行规则<rowKeyRule>未设置. 默认为:" + rowkey);
			return rowkey;
		}

		return param;
	}

	/**
	 * 获取HFile的最大值
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	private int getParamHfileMaxfilesize(Map<String, String> paraMap) throws Exception {
		String param = paraMap.get("hfileMaxfilesize");
		if (null == param || param.trim().length() <= 0) {
			LOG.warn("[MR WARN]HFile的最大值<hfileMaxfilesize>未设置. 默认为: 256 * 1024 * 1024");
			return 256 * 1024 * 1024;
		}

		try {
			return Integer.parseInt(param);
		} catch (Exception e) {
			String meg = "[MR ERROR]HFile的最大值<hfileMaxfilesize>设置错误.";
			LOG.error(meg);
			throw new Exception(meg);
		}
	}

	/**
	 * 获取压缩类型
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	private int getParamCompressionType(Map<String, String> paraMap) throws Exception {
		String param = paraMap.get("compressionType");
		if (null == param || param.trim().length() <= 0) {
			LOG.warn("[MR WARN]压缩类型<compressionType>未设置.默认为0");
			return 0;
		}

		try {
			return Integer.parseInt(param);
		} catch (Exception e) {
			String meg = "[MR ERROR]压缩类型<compressionType>设置错误.";
			LOG.error(meg);
			throw new Exception(meg);
		}
	}

	private int getParamCompactionCompressionType(Map<String, String> paraMap) throws Exception {
		String param = paraMap.get("compactionCompressionType");
		if (null == param || param.trim().length() <= 0) {
			LOG.warn("[MR WARN]合并压缩类型<compactionCompressionType>未设置.默认为0");
			return 0;
		}

		try {
			return Integer.parseInt(param);
		} catch (Exception e) {
			String meg = "[MR ERROR]合并压缩类型<compactionCompressionType>设置错误.";
			LOG.error(meg);
			throw new Exception(meg);
		}
	}

	/**
	 * 获取列的块大小
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	private int getParamColmunBlocksize(Map<String, String> paraMap) throws Exception {
		String param = paraMap.get("colmunBlocksize");
		if (null == param || param.trim().length() <= 0) {
			LOG.warn("[MR WARN]列的块大小<colmunBlocksize>未设置.默认为0");
			return 0;
		}

		try {
			return Integer.parseInt(param);
		} catch (Exception e) {
			String meg = "[MR ERROR]列的块大小<colmunBlocksize>设置错误.";
			LOG.error(meg);
			throw new Exception(meg);
		}
	}

	/**
	 * 获取列的最大版本号
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	private int getParamColmunMaxversion(Map<String, String> paraMap) throws Exception {
		String param = paraMap.get("colmunMaxversion");
		if (null == param || param.trim().length() <= 0) {
			LOG.warn("[MR WARN]列的最大版本号<colmunMaxversion>未设置.默认为3");
			return 3;
		}

		try {
			return Integer.parseInt(param);
		} catch (Exception e) {
			String meg = "[MR ERROR]列的最大版本号<colmunMaxversion>设置错误.";
			LOG.error(meg);
			throw new Exception(meg);
		}
	}

	/**
	 * 获取列的最小版本号
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	private int getParamColmunMinversion(Map<String, String> paraMap) throws Exception {
		String param = paraMap.get("colmunMinversion");
		if (null == param || param.trim().length() <= 0) {
			LOG.warn("[MR WARN]列的最小版本号<colmunMinversion>未设置.默认为1");
			return 1;
		}

		try {
			return Integer.parseInt(param);
		} catch (Exception e) {
			String meg = "[MR ERROR]列的最小版本号<colmunMinversion>设置错误.";
			LOG.error(meg);
			throw new Exception(meg);
		}
	}

	/**
	 * 获取目标列族和列的对应关系
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	private String getParamTargetFamilyNames(Map<String, String> paraMap) throws Exception {
		String param = paraMap.get("targetFamilyNames");
		if (null == param || param.trim().length() <= 0) {
			String meg = "[MR ERROR]目标列族和列的对应关系<targetFamilyNames>未设置.";
			LOG.error(meg);
			throw new Exception(meg);
		}

		Set<String> setTargetFiled = this.getParamTargetFieldNameList(paraMap);
		String famliyNames[] = param.split(",");
		for (String fn : famliyNames) {
			String fc[] = fn.split(":");
			if (fc.length != 2 || fc[0].trim().length() <= 0 || fc[1].trim().length() <= 0) {
				String meg = "[MR ERROR]目标列族和列的对应关系<targetFamilyNames>设置错误.";
				LOG.error(meg);
				throw new Exception(meg);
			}

			if (!setTargetFiled.contains(fc[1])) {
				String meg = "[MR ERROR]目标列族和列的对应关系<targetFamilyNames>设置错误.列族与列=>" + fn + ",不存在与目标字段列表内.";
				LOG.error(meg);
				throw new Exception(meg);
			}
		}

		return param;
	}

	/**
	 * 测试入口函数
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		MRFileToHDBService s = new MRFileToHDBService();
		s.run(testNoReduceParamter());
	}

	/**
	 * 包含统计方法的从文件输出HBase
	 * 
	 * @return
	 */
	protected static Map<String, String> testMRParamter() {
		String resource = "hbase-default.xml,hbase-site.xml";// 以逗号隔开
		String jobName = "M-R File to HDB";
		String jobPriority = "HIGH";
		String hdfsBaseUrl = "hdfs://hadoop01:9000";
		String jobTrackerHost = "hadoop01:9001";
		String dirInput = "/wanghao/isomerism";
		String dirOutput = "/wanghao/isomerism-result";
		String isFilter = "false"; // 是否需要过滤，true表示启用过滤，否则不启用过滤
		String regFilterFile = "mrdata.txt";// 过滤正则表达式
		String regFilterType = "false";
		String isinputMapEnd = "false"; // 需要map直接输出

		// 输入参数
		String inputFieldNames = "time,id,type,price,number,message"; // 输入字段
		String inputFileFieldSplitChars = "\\^";
		String inputFileRowsSplitChars = "\n";
		String isCombinerClass = "true";

		// 输出参数
		String targetTableName = "t_mrhbase"; // 目标表名(HBase Table)
		String targetFamilyNames = "f1:sid,f1:sprice,f1:mprice,f2:avgnumber,f2:sumnumber,f2:rowcounts,f2:stime"; // 目标列族名称(family:column)
		String targetFieldNames = "sid,sprice,mprice,avgnumber,sumnumber,rowcounts,stime";// 输出字段
		String rowKeyRule = "tj-{sid}-{stime, yyyy-MM-dd}"; // rowkey规则
		String hfileMaxfilesize = String.valueOf(256 * 1024 * 1024); // hfile最大值
		String compressionType = String.valueOf(HbaseConfiguration.COMPRESSION_ALGORITHM_GZ);// 压缩类型
		String compactionCompressionType = String.valueOf(HbaseConfiguration.COMPRESSION_ALGORITHM_GZ);// 合并压缩类型
		String colmunBlocksize = String.valueOf(64 * 1024); // 块大小
		String colmunMaxversion = "6"; // 最大版本号
		String colmunMinversion = "1"; // 最小版本号

		String numMapTasks = "5"; // map任务数
		String numReduceTasks = "1"; // reduce任务数
		String groupFieldMethod = "sid:id:NONE,stime:time:NONE,sprice:price:MIN,mprice:price:MAX,avgnumber:number:AVG,sumnumber:number:SUM,rowcounts:id:COUNT";

		Map<String, String> paraMap = new HashMap<String, String>();
		// 系统参数
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

		// 输入参数
		paraMap.put("inputFieldNames", inputFieldNames);
		paraMap.put("inputFileFieldSplitChars", inputFileFieldSplitChars);
		paraMap.put("inputFileRowsSplitChars", inputFileRowsSplitChars);
		paraMap.put("isCombinerClass", isCombinerClass);

		// 输出参数
		paraMap.put("targetTableName", targetTableName);
		paraMap.put("targetFamilyNames", targetFamilyNames);
		paraMap.put("targetFieldNames", targetFieldNames);
		paraMap.put("rowKeyRule", rowKeyRule);
		paraMap.put("hfileMaxfilesize", hfileMaxfilesize);
		paraMap.put("compressionType", compressionType);
		paraMap.put("compactionCompressionType", compactionCompressionType);
		paraMap.put("colmunBlocksize", colmunBlocksize);
		paraMap.put("colmunMaxversion", colmunMaxversion);
		paraMap.put("colmunMinversion", colmunMinversion);

		// 设置任务数
		paraMap.put("numMapTasks", numMapTasks);
		paraMap.put("numReduceTasks", numReduceTasks);

		// 设置输入与输出关系
		paraMap.put("groupFieldMethod", groupFieldMethod);

		return paraMap;
	}

	/**
	 * 包含不含统计方法的从文件输出HBase
	 * 
	 * @return
	 */
	protected static Map<String, String> testNoReduceParamter() {
		String resource = "hbase-default.xml,hbase-site.xml";// 以逗号隔开
		String jobName = "M-R File to HDB(noreduce)";
		String jobPriority = "HIGH";
		String hdfsBaseUrl = "hdfs://hadoop01:9000";
		String jobTrackerHost = "hadoop01:9001";
		String dirInput = "/wanghao/isomerism";
		String dirOutput = "/wanghao/isomerism-result";
		String isFilter = "false"; // 是否需要过滤，true表示启用过滤，否则不启用过滤
		String regFilterFile = "mrdata.txt";// 过滤正则表达式
		String regFilterType = "false";
		String isinputMapEnd = "true"; // 需要map直接输出

		// 输入参数
		String inputFieldNames = "time,id,type,price,number,message"; // 输入字段
		String inputFileFieldSplitChars = "\\^";
		String inputFileRowsSplitChars = "\n";
		String isCombinerClass = "true";

		// 输出参数
		String targetTableName = "t_mrnogrouphbase"; // 目标表名(HBase Table)
		String targetFamilyNames = "f1:sid,f1:sprice,f2:snumber,f2:smessage,f2:stime"; // 目标列族名称(family:column)
		String targetFieldNames = "sid,sprice,snumber,smessage,stime";// 输出字段
		String rowKeyRule = "tj-{sid}-{stime, yyyy-MM-dd}"; // rowkey规则
		String hfileMaxfilesize = String.valueOf(256 * 1024 * 1024); // hfile最大值
		String compressionType = String.valueOf(HbaseConfiguration.COMPRESSION_ALGORITHM_GZ);// 压缩类型
		String compactionCompressionType = String.valueOf(HbaseConfiguration.COMPRESSION_ALGORITHM_GZ);// 合并压缩类型
		String colmunBlocksize = String.valueOf(64 * 1024); // 块大小
		String colmunMaxversion = "3"; // 最大版本号
		String colmunMinversion = "1"; // 最小版本号

		String numMapTasks = "5"; // map任务数
		String numReduceTasks = "0"; // reduce任务数
		String targetToSrcField = "sid:id,stime:time,sprice:price,snumber:number,smessage:message";

		Map<String, String> paraMap = new HashMap<String, String>();
		// 系统参数
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

		// 输入参数
		paraMap.put("inputFieldNames", inputFieldNames);
		paraMap.put("inputFileFieldSplitChars", inputFileFieldSplitChars);
		paraMap.put("inputFileRowsSplitChars", inputFileRowsSplitChars);
		paraMap.put("isCombinerClass", isCombinerClass);

		// 输出参数
		paraMap.put("targetTableName", targetTableName);
		paraMap.put("targetFamilyNames", targetFamilyNames);
		paraMap.put("targetFieldNames", targetFieldNames);
		paraMap.put("rowKeyRule", rowKeyRule);
		paraMap.put("hfileMaxfilesize", hfileMaxfilesize);
		paraMap.put("compressionType", compressionType);
		paraMap.put("compactionCompressionType", compactionCompressionType);
		paraMap.put("colmunBlocksize", colmunBlocksize);
		paraMap.put("colmunMaxversion", colmunMaxversion);
		paraMap.put("colmunMinversion", colmunMinversion);

		// 设置任务数
		paraMap.put("numMapTasks", numMapTasks);
		paraMap.put("numReduceTasks", numReduceTasks);

		// 设置输入与输出关系
		paraMap.put("targetToSrcField", targetToSrcField);

		return paraMap;
	}
}