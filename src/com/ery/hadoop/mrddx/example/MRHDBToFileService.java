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
import com.ery.hadoop.mrddx.DBMapper;
import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.DBReducer;
import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.file.TextOutputFormat;
import com.ery.hadoop.mrddx.hbase.HbaseConfiguration;
import com.ery.hadoop.mrddx.hbase.HbaseInputFormat;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.HDFSUtils;

/**

 * 

 * @version v1.0
 */
public class MRHDBToFileService extends MRTestJOBService {
	public void run(Map<String, String> paraMap) throws Exception {
		// 获取系统参数
		String pResource[] = this.getParamSysResource(paraMap);// 获取资源文件
		String pJobName = this.getParamSysJobName(paraMap);// 获取job名称
		JobPriority pJobPriority = this.getParamSysJobPriority(paraMap);// 获取job优先级
		String pHdfsBaseUrl = this.getParamSysHdfsBaseUrl(paraMap);// 获取hdfsBaseUrl文件系统地址
		String pJobTrackerHost = this.getParamSysJobTrackerHost(paraMap);// 获取jobhost
		String pDirInput = this.getParamSysDirInput(paraMap);// 获取输入路径
		String pDirOutput = this.getParamSysDirOutput(paraMap);// 获取输出路径

		// 输入参数
		String pInputTableName = this.getParamInputTableName(paraMap);
		String pInputFieldNames = this.getParamInputFieldNames(paraMap);// 获取源字段
		String pSrcTargetFieldNames = this.getParamSrcTargetFieldNames(paraMap);// 获取列与源字段对应关系
		String pGroupFieldMethod = this.getParamRelatedGroupFieldMethod(paraMap);// 获取目标字段:源字段:统计方法
		boolean pIsCombinerClass = this.getParamInputIsCombinerClass(paraMap);// 获取是否在map中合并

		long[] pTimerange = this.getParamQueryTimeRange(paraMap);
		String pStartrow = this.getParamQueryStartRow(paraMap);
		String pStoprow = this.getParamQueryStopRow(paraMap);
		long pTimestamp = this.getParamQueryTimeStamp(paraMap);
		String[] pFilters = this.getParamQueryFilters(paraMap);
		String[] pFamilyColumns = this.getParamQueryFamilyColumns(paraMap);
		String[] pFamilys = this.getParamQueryFamilys(paraMap);

		// 输出参数
		String pTargetFieldNames[] = this.getParamOutputFieldNames(paraMap);// 获取目标字段
		boolean pTargetCompress = this.getParamOutputCompress(paraMap);// 获取目标文件是否压缩
		String pTargetCompressCodec = this.getParamOutputCompressCodec(paraMap);// 获取目标文件压缩方式
		String pOutputFileFieldSplitChars = this.getParamOutputFileFieldSplitChars(paraMap);// 获取目标文件列分隔符
		String pOutputFileRowsSplitChars = this.getParamOutputFileRowsSplitChars(paraMap);// 获取目标文件行分隔符
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
			job.setJarByClass(MRHDBToFileService.class);

			job.setJobName(pJobName);
			job.setPriority(pJobPriority);
			conf.set(MRConfiguration.FS_DEFAULT_NAME, pHdfsBaseUrl);
			conf.set(MRConfiguration.MAPRED_JOB_TRACKER, pJobTrackerHost);

			FileInputFormat.setInputPaths(job, inPaths);
			FileOutputFormat.setOutputPath(job, outPath);
			job.setMapperClass(DBMapper.class);
			job.setReducerClass(DBReducer.class);

			// input from hdb
			job.setMapOutputKeyClass(DBRecord.class);
			job.setMapOutputValueClass(DBRecord.class);

			// hbase conf
			HbaseConfiguration hbaseConf = new HbaseConfiguration(conf, HbaseConfiguration.FLAG_HBASE_INPUT);
			hbaseConf.setInputFieldNames(pInputFieldNames);// 设置输入字段顺序,与文件中顺序对应
			hbaseConf.setRelatedGroupFieldMethod(pGroupFieldMethod);
			if (pIsCombinerClass) {
				job.setCombinerClass(DBGroupReducer.class);
			}

			// input from HBase
			// 设置表名等信息
			HbaseInputFormat.setInput(job, DBRecord.class, pInputTableName, pSrcTargetFieldNames);
			// 设置查询条件：TIMERANGE, , STARTROW, STOPROW, TIMESTAMP, FILTER,
			// COLUMNS, familys,
			HbaseInputFormat.setInputQueryCondition(conf, pTimerange, pStartrow, pStoprow, pTimestamp, pFilters,
					pFamilyColumns, pFamilys);

			// 获取MapTask的数量
			int taskNumber = HbaseInputFormat.getTableHRegionInfoCount(conf, pStartrow, pStoprow);
			int reduceTasks = taskNumber;
			MRLog.info(LOG, "MapTasks的数量:" + taskNumber);
			MRLog.info(LOG, "ReduceTasks的数量:" + reduceTasks);

			// output to file
			job.setOutputFormatClass(TextOutputFormat.class);// 输出到文本文件，保留原有压缩设置
			TextOutputFormat.setOutputParameter(job, pTargetCompress, pTargetCompressCodec, pOutputFileFieldSplitChars,
					pOutputFileRowsSplitChars);
			hbaseConf.setOutputFieldNames(pTargetFieldNames);

			new MRConfiguration(conf).setNumMapTasks(taskNumber);
			job.setNumReduceTasks(reduceTasks);
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
	 * 获取表名称
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	private String getParamInputTableName(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("inputTableName");
		if (null == para || para.trim().length() <= 0) {
			String meg = "[MR ERROR]HBase表名称<inputTableName>不能为空.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		return para;
	}

	/**
	 * 获取列与自定义源字段
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	private String getParamSrcTargetFieldNames(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("srcTargetFieldNames");
		if (null == para || para.trim().length() <= 0) {
			String meg = "[MR ERROR]HBase列与自定义源字段<srcTargetFieldNames>不能为空.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		Set<String> setSrcfield = getParamSrcFieldNameList(paraMap);
		String fcd[] = para.split(",");
		for (String tmp : fcd) {
			String src[] = tmp.split(":");
			if (src.length != 2) {
				String meg = "[MR ERROR]HBase列与自定义源字段<srcTargetFieldNames>配置错误.";
				MRLog.error(LOG, meg);
				throw new Exception(meg);
			}

			if (!setSrcfield.contains(src[1])) {
				String meg = "[MR ERROR]HBase列与自定义源字段<srcTargetFieldNames>配置错误.自定义字段:" + src[1] + "不包含在源字段中.";
				MRLog.error(LOG, meg);
				throw new Exception(meg);
			}
		}

		return para;
	}

	/**
	 * 获取查询条件的时间范围
	 * 
	 * @param paraMap
	 * @return 数组为0表示不需要
	 * @throws Exception
	 */
	private long[] getParamQueryTimeRange(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("queryTimeRange");
		if (null == para || para.trim().length() <= 0) {
			// 不包含在查询条件中.
			MRLog.warn(LOG, "[MR WARN]查询条件的时间范围<queryTimeRange>未设置.");
			return new long[0];
		}

		String range[] = para.split(",");
		if (range.length != 2) {
			String meg = "[MR ERROR]HBase查询条件的时间范围<queryTimeRange>设置错误,比如:132342155,32423532.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		long trange[] = new long[2];
		try {
			trange[0] = Long.parseLong(range[0]);
			trange[1] = Long.parseLong(range[1]);
		} catch (Exception e) {
			String meg = "[MR ERROR]HBase查询条件的时间范围<queryTimeRange>设置错误,可能为空或不是数字.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		if (trange[0] > trange[1]) {
			String meg = "[MR ERROR]HBase查询条件的时间范围<queryTimeRange>设置错误,起始时间大于结束时间.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		return trange;
	}

	/**
	 * 获取查询条件的startrow
	 * 
	 * @param paraMap
	 * @return
	 */
	private String getParamQueryStartRow(Map<String, String> paraMap) {
		String para = paraMap.get("queryStartRow");
		if (null == para || para.trim().length() <= 0) {
			MRLog.warn(LOG, "[MR WARN]查询条件的startrow<queryStartRow>未设置.");
			return null;
		}

		return para;
	}

	/**
	 * 获取查询条件的stoprow
	 * 
	 * @param paraMap
	 * @return
	 */
	private String getParamQueryStopRow(Map<String, String> paraMap) {
		String para = paraMap.get("queryStopRow");
		if (null == para || para.trim().length() <= 0) {
			MRLog.warn(LOG, "[MR WARN]查询条件的stoprow<queryStopRow>未设置.");
			return null;
		}

		return para;
	}

	/**
	 * 获取查询条件的时间戳
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	private long getParamQueryTimeStamp(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("queryTimestamp");
		if (null == para || para.trim().length() <= 0) {
			MRLog.warn(LOG, "[MR WARN]查询条件的时间戳<queryTimestamp>未设置.");
			return -1;
		}

		try {
			return Long.parseLong(para);
		} catch (Exception e) {
			String meg = "[MR ERROR]查询条件的时间戳<queryTimeRange>设置错误,不是数字.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}
	}

	/**
	 * 获取查询条件的过滤条件
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	private String[] getParamQueryFilters(Map<String, String> paraMap) {
		String para = paraMap.get("queryFilters");
		if (null == para || para.trim().length() <= 0) {
			MRLog.warn(LOG, "[MR WARN]查询条件的过滤条件<queryFilters>未设置.");
			return new String[0];
		}

		return para.split(",");
	}

	/**
	 * 获取查询条件的列
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	private String[] getParamQueryFamilyColumns(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("queryFamilyColumns");
		if (null == para || para.trim().length() <= 0) {
			MRLog.warn(LOG, "[MR WARN]查询条件的列<queryFamilyColumns>未设置.");
			return new String[0];
		}

		String familyColumns[] = para.split(",");
		for (String tmp : familyColumns) {
			if (tmp.split(":").length != 2) {
				String meg = "[MR ERROR]查询条件的列<queryFamilyColumns>设置错误.";
				MRLog.error(LOG, meg);
				throw new Exception(meg);
			}
		}

		return familyColumns;
	}

	/**
	 * 获取查询条件的列族
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	private String[] getParamQueryFamilys(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("queryFamilys");
		if (null == para || para.trim().length() <= 0) {
			MRLog.warn(LOG, "[MR WARN]查询条件的列族<queryFamilys>未设置.");
			return new String[0];
		}

		return para.split(",");
	}

	/**
	 * 入口函数
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		MRHDBToFileService mr = new MRHDBToFileService();
		Map<String, String> param = testNoReduceParamter();
		// Map<String, String> param = testMRParamter();
		mr.run(param);
	}

	public static Map<String, String> testMRParamter() {
		// 系统参数
		String resource = "hbase-default.xml,hbase-site.xml";// 以逗号隔开
		String jobName = "M-R HDB to File";
		String jobPriority = "HIGH";
		String hdfsBaseUrl = "hdfs://hadoop01:9000";
		String jobTrackerHost = "hadoop01:9001";
		String dirInput = "/wanghao/isomerism";
		String dirOutput = "/wanghao/isomerism-result";
		String regFilterFile = "mrdata.txt";// 过滤正则表达式
		String isFilter = "false"; // 是否需要过滤，true表示启用过滤，否则不启用过滤
		String regFilterType = "false";
		String isinputMapEnd = "false"; // 需要map直接输出

		// 输入
		String inputTableName = "t_mrnogrouphbase";// hbase表名
		String inputFieldNames = "time,id,price,message,number"; // 输入字段
		String srcTargetFieldNames = "f1-sid:id,f1-sprice:price,f2-smessage:message,f2-snumber:number,f2-stime:time"; // hbase源字段与定义名称，格式(family-column:definedSrcFileName,......)，并且自定义的字段名称要与srcFieldNames中对应
		String isCombinerClass = "false"; // map是否合

		String queryTimerange = "1357269833605,1357269833710"; // 格式(起始时间:结束时间)
		String queryStartrow = "tj-1000-2012-12-27";
		String queryStoprow = "tj-1006-2012-12-27";
		// String queryTimestamp = "1357269833558";// 与queryTimerange条件只能选其一
		String queryFilters = "filterType:17;family:f2;qualifier:snumber;CompareOp:EQUAL;value:739"; // 以SingleColumnValueFilter为例，多个以","分割
		// String queryFamilyColumns = "f1:sprice"; //
		// 格式(family1:column1,family1:column2)
		// String queryFamilys = "f1"; // 格式(family1:family2)

		// 输出
		String targetFilePath = "/wanghao/isomerism-hiveToFile";
		String targetFieldNames = "sid,minprice,maxprice,smessage,rowcounts";
		String outputFileFieldSplitChars = ",";
		String outputFileRowsSplitChars = "\n";
		String targetCompress = "false";
		String targetCompressCodec = HDFSUtils.CompressCodec.GzipCodec.toString();

		// 目标字段(targFieldNames):源字段(srcFieldNames):统计源字段的方法
		String groupFieldMethod = "sid:id:NONE,minprice:price:MIN,maxprice:price:MAX,smessage:message:NONE,rowcounts:id:COUNT";

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
		paraMap.put("inputTableName", inputTableName);
		paraMap.put("srcTargetFieldNames", srcTargetFieldNames);
		paraMap.put("inputFieldNames", inputFieldNames);
		paraMap.put("isCombinerClass", isCombinerClass);
		paraMap.put("queryTimeRange", queryTimerange);
		paraMap.put("queryStartRow", queryStartrow);
		paraMap.put("queryStopRow", queryStoprow);
		// paraMap.put("queryTimestamp", queryTimestamp);
		paraMap.put("queryFilters", queryFilters);
		// paraMap.put("queryFamilyColumns", queryFamilyColumns);
		// paraMap.put("queryFamilys", queryFamilys);

		// 设置输出
		paraMap.put("targetFieldNames", targetFieldNames);
		paraMap.put("outputFileFieldSplitChars", outputFileFieldSplitChars);
		paraMap.put("outputFileRowsSplitChars", outputFileRowsSplitChars);
		paraMap.put("targetCompress", targetCompress);
		paraMap.put("targetCompressCodec", targetCompressCodec);
		paraMap.put("targetFilePath", targetFilePath);

		// 设置输入与输出关系
		paraMap.put("groupFieldMethod", groupFieldMethod);

		return paraMap;
	}

	public static Map<String, String> testNoReduceParamter() {
		// 系统参数
		String resource = "hbase-default.xml,hbase-site.xml";// 以逗号隔开
		String jobName = "M-R HDB to File(noreduce)";
		String jobPriority = "HIGH";
		String hdfsBaseUrl = "hdfs://hadoop01:9000";
		String jobTrackerHost = "hadoop01:9001";
		String dirInput = "/wanghao/isomerism";
		String dirOutput = "/wanghao/isomerism-result";
		String regFilterFile = "mrdata.txt";// 过滤正则表达式
		String isFilter = "false"; // 是否需要过滤，true表示启用过滤，否则不启用过滤
		String regFilterType = "false";
		String isinputMapEnd = "true"; // 需要map直接输出

		// 输入
		String inputTableName = "t_mrnogrouphbase";// hbase表名
		String inputFieldNames = "time,id,price,message,number"; // 输入字段
		String srcTargetFieldNames = "f1-sid:id,f1-sprice:price,f2-smessage:message,f2-snumber:number,f2-stime:time"; // hbase源字段与定义名称，格式(family-column:definedSrcFileName,......)，并且自定义的字段名称要与srcFieldNames中对应
		String isCombinerClass = "false"; // map是否合

		String queryTimerange = "1357269833605,1357269833710"; // 格式(起始时间:结束时间)
		String queryStartrow = "tj-1000-2012-12-27";
		String queryStoprow = "tj-1006-2012-12-27";
		// String queryTimestamp = "1357269833558";// 与queryTimerange条件只能选其一
		String queryFilters = "filterType:17;family:f2;qualifier:snumber;CompareOp:EQUAL;value:739"; // 以SingleColumnValueFilter为例，多个以","分割
		// String queryFamilyColumns = "f1:sid,f1:sprice,f2:smessage"; //
		// 格式(family1:column1,family1:column2)
		// String queryFamilys = "f1,f2"; // 格式(family1:family2)

		// 输出
		String targetFilePath = "/wanghao/isomerism-hiveToFile";
		String targetFieldNames = "sid,sprice,smessage";
		String outputFileFieldSplitChars = ",";
		String outputFileRowsSplitChars = "\n";
		String targetCompress = "false";
		String targetCompressCodec = HDFSUtils.CompressCodec.GzipCodec.toString();

		// 输入输出关系
		String targetToSrcField = "sid:id,sprice:price,smessage:message";// 目标字段:源字段

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
		paraMap.put("inputTableName", inputTableName);
		paraMap.put("srcTargetFieldNames", srcTargetFieldNames);
		paraMap.put("inputFieldNames", inputFieldNames);
		paraMap.put("isCombinerClass", isCombinerClass);
		paraMap.put("queryTimeRange", queryTimerange);
		paraMap.put("queryStartRow", queryStartrow);
		paraMap.put("queryStopRow", queryStoprow);
		// paraMap.put("queryTimestamp", queryTimestamp);
		paraMap.put("queryFilters", queryFilters);
		// paraMap.put("queryFamilyColumns", queryFamilyColumns);
		// paraMap.put("queryFamilys", queryFamilys);

		// 设置输出
		paraMap.put("targetFieldNames", targetFieldNames);
		paraMap.put("outputFileFieldSplitChars", outputFileFieldSplitChars);
		paraMap.put("outputFileRowsSplitChars", outputFileRowsSplitChars);
		paraMap.put("targetCompress", targetCompress);
		paraMap.put("targetCompressCodec", targetCompressCodec);
		paraMap.put("targetFilePath", targetFilePath);

		// 设置输入与输出关系
		paraMap.put("targetToSrcField", targetToSrcField);

		return paraMap;
	}
}