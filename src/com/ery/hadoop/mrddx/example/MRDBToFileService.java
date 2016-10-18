package com.ery.hadoop.mrddx.example;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.ery.hadoop.mrddx.DBGroupReducer;
import com.ery.hadoop.mrddx.DBMapper;
import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.DBReducer;
import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.db.DBConfiguration;
import com.ery.hadoop.mrddx.db.DBInputFormat;
import com.ery.hadoop.mrddx.db.mapreduce.DataDrivenDBInputFormat;
import com.ery.hadoop.mrddx.db.mapreduce.OracleDataDrivenDBInputFormat;
import com.ery.hadoop.mrddx.file.TextOutputFormat;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.HDFSUtils;

/**
 * Copyrights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights reserved.
 * 
 * @Project tydic hadoop
 * @Comments 数据库(mysql,oracle) 到文件
 * @author wanghao
 * @version v1.0
 * @create Data 2013-1-7
 */
public class MRDBToFileService extends MRTestJOBService {
	// 拆分类型：limit offset 记录数的拆分
	public static final int SPLITTYPE_LIMIT_OFFSET = 1;

	// 拆分类型：where条件 记录数的拆分
	public static final int SPLITTYPE_WHERE = 2;

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
		String pInputDBDriverClass = this.getParamInputDBDriverClass(paraMap);// 数据库驱动
		String pInputDBUrl = this.getParamInputDBUrl(paraMap);// 数据地址
		String pInputDBUserName = this.getParamInputDBUserName(paraMap);// 数据库用户名
		String pInputDBPasswd = this.getParamInputDBPasswd(paraMap);// 数据库
		int pInputSplitType = this.getParamInputDBSplitType(paraMap);// 获取拆分类型(只针对源数据是数据库而言)

		String pInputFieldNames = this.getParamInputFieldNames(paraMap);// 获取源字段
		String pGroupFieldMethod = this.getParamRelatedGroupFieldMethod(paraMap);// 获取目标字段:源字段:统计方法
		boolean pIsCombinerClass = this.getParamInputIsCombinerClass(paraMap);// 获取是否在map中合并

		String pInputQuery = this.getParamInputQuery(paraMap);// 获取输入的查询sql
		String pInputCountQuery = this.getParamInputCountQuery(paraMap);// 获取输入的查询记录总数的sql
		int pInputQueryfetchSize = this.getParamInputQueryfetchSize(paraMap); // 设置查询的缓冲大小
		String pInputTableName = this.getParamInputTableName(paraMap);// 获取源表名
		String pInputConditions = this.getparamInputConditions(paraMap);// 获取查询条件
		String pInputBoundingQuery = this.getParamInputBoundingQuery(paraMap);// 获取
		String pInputOrderByColumn = this.getParamInputOrderByColumn(paraMap);// 获取排序条件

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
			Job conf = Job.getInstance();
			conf.setJarByClass(MRDBToFileService.class);
			String[] dirInputs = pDirInput.split(",");
			Path inPaths[] = new Path[dirInputs.length];
			for (int i = 0; i < dirInputs.length; i++) {
				inPaths[i] = new Path(dirInputs[i]);
			}
			FileSystem fs = FileSystem.get(conf.getConfiguration());
			Path outPath = new Path(pDirOutput);
			if (fs.exists(outPath)) {
				fs.delete(outPath, true);
			}

			for (int i = 0; i < pResource.length; i++) {
				conf.getConfiguration().addResource(pResource[i]);
			}
			conf.setJobName(pJobName);
			conf.getConfiguration().set(JobContext.PRIORITY, pJobPriority.toString());
			conf.getConfiguration().set(MRConfiguration.FS_DEFAULT_NAME, pHdfsBaseUrl);
			conf.getConfiguration().set(MRConfiguration.MAPRED_JOB_TRACKER, pJobTrackerHost);

			FileInputFormat.setInputPaths(conf, inPaths);
			FileOutputFormat.setOutputPath(conf, outPath);
			conf.setMapperClass(DBMapper.class);
			conf.setReducerClass(DBReducer.class);

			// 输入设置
			conf.setMapOutputKeyClass(DBRecord.class);
			conf.setMapOutputValueClass(DBRecord.class);

			DBConfiguration dbconf = new DBConfiguration(conf.getConfiguration());
			dbconf.setRelatedGroupFieldMethod(pGroupFieldMethod);
			if (pIsCombinerClass) {
				conf.setCombinerClass(DBGroupReducer.class);
				conf.setCombinerKeyGroupingComparatorClass(DBRecord.class);
			}

			// 以下按拆分方式处理(默认为1)
			switch (pInputSplitType) {
			case SPLITTYPE_LIMIT_OFFSET: // 1:按记录数进行拆分(默认方式)
				conf.setInputFormatClass(DBInputFormat.class);
				DBConfiguration.configureInputDB(conf.getConfiguration(), pInputDBDriverClass, pInputDBUrl,
						pInputDBUserName, pInputDBPasswd);// 设置数据库配置
				DBInputFormat.setInput(conf.getConfiguration(), DBRecord.class, pInputQuery, pInputCountQuery,
						pInputQueryfetchSize);// 设置查询
				// 设置数据拆分相关查询(表名、字段名、条件、排序）
				DBInputFormat.setInput(conf.getConfiguration(), pInputTableName, pInputFieldNames, pInputConditions,
						pInputBoundingQuery, pInputOrderByColumn);
				break;
			case SPLITTYPE_WHERE: // 2:不按记录数进行拆分,使用数据类型驱动拆分
				String produceName = dbconf.getDBProduceName(dbconf.getInputConnection(pInputDBDriverClass,
						pInputDBUrl, pInputDBUserName, pInputDBPasswd));
				// ORACLE
				if (null != produceName && "ORACLE".equals(produceName)) {
					conf.setInputFormatClass(OracleDataDrivenDBInputFormat.class);
					DBConfiguration.configureInputDB(conf.getConfiguration(), pInputDBDriverClass, pInputDBUrl,
							pInputDBUserName, pInputDBPasswd);// 设置数据库配置
					OracleDataDrivenDBInputFormat.setInput(conf.getConfiguration(), DBRecord.class, pInputQuery, null,
							pInputQueryfetchSize);// 设置查询
					// 设置数据拆分相关查询(表名、条件、排序）
					OracleDataDrivenDBInputFormat.setInput(conf.getConfiguration(), pInputTableName, pInputFieldNames,
							pInputConditions, pInputBoundingQuery, pInputOrderByColumn);
					break;
				}

				conf.setInputFormatClass(DataDrivenDBInputFormat.class);
				DBConfiguration.configureInputDB(conf.getConfiguration(), pInputDBDriverClass, pInputDBUrl,
						pInputDBUserName, pInputDBPasswd);// 设置数据库配置
				DataDrivenDBInputFormat.setInput(conf.getConfiguration(), DBRecord.class, pInputQuery, null,
						pInputQueryfetchSize);// 设置查询
				// 设置数据拆分相关查询(表名、条件、排序）
				DataDrivenDBInputFormat.setInput(conf.getConfiguration(), pInputTableName, pInputFieldNames,
						pInputConditions, pInputBoundingQuery, pInputOrderByColumn);
				break;
			default:

				break;
			}

			// 验证输入参数
			this.validateInputParameter(pInputSplitType, pInputQuery, pInputCountQuery, pInputTableName,
					pInputFieldNames, pInputConditions, pInputBoundingQuery, pInputOrderByColumn);

			// 输出设置
			conf.setOutputFormatClass(TextOutputFormat.class);// 输出到文本文件，保留原有压缩设置
			TextOutputFormat.setOutputParameter(conf, pTargetCompress, pTargetCompressCodec,
					pOutputFileFieldSplitChars, pOutputFileRowsSplitChars);
			dbconf.setOutputFieldNames(pTargetFieldNames);

			// NUM_MAPS = "mapreduce.job.maps"
			dbconf.setNumMapTasks(pNumMapTasks);
			// conf.setNumMapTasks(pNumMapTasks);
			conf.setNumReduceTasks(pNumReduceTasks);
			conf.setOutputKeyClass(DBRecord.class);
			conf.setOutputValueClass(NullWritable.class);
			conf.waitForCompletion(true);
			// JobClient.runJob(conf);
			// 将文件从临时目录拷贝到目标目录下
			HDFSUtils.mvFiles(conf.getConfiguration(), pDirOutput + "/part-*", ptargetFilePath);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected String getParamInputFieldNames(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("inputFieldNames");
		if (null == para || para.trim().length() <= 0) {
			String tableName = this.getParamInputTableName(paraMap);
			if (null == tableName || tableName.trim().length() <= 0) {
				String meg = "在源字段<inputFieldNames>未设置时，表名<inputTableName>必须设置";
				MRLog.error(LOG, meg);
				throw new Exception(meg);
			}
			return "";
		}

		return para; // 输入字段
	}

	/**
	 * 获取拆分类型
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	private int getParamInputDBSplitType(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("inputDBSplitType");
		if (null == para || para.trim().length() <= 0) {
			LOG.warn("拆分类型<inputDBSplitType>未设置，默认为 1(limit-offset拆分)");
			return 1;
		}

		int type = 0;
		try {
			type = Integer.valueOf(para);
		} catch (Exception e) {
			String meg = "拆分类型<splitType>数据异常,当前为:" + type + ", 取值范围:1,2";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		if (type == 1 || type == 2) {
			return type;
		}

		String meg = "拆分类型<splitType>数据异常,当前为:" + type + ", 取值范围:1,2";
		MRLog.error(LOG, meg);
		throw new Exception(meg);
	}

	/**
	 * 获取数据库驱动
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	private String getParamInputDBDriverClass(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("inputDBDriverClass");
		if (null == para || para.trim().length() <= 0) {
			String meg = "数据库驱动<inputDBDriverClass>未设置.";
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
	private String getParamInputDBUrl(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("inputDBUrl");
		if (null == para || para.trim().length() <= 0) {
			String meg = "数据库地址<inputDBUrl>未设置.";
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
	 * @throws Exception
	 */
	private String getParamInputDBUserName(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("inputDBUserName");
		if (null == para || para.trim().length() <= 0) {
			String meg = "数据库用户名<inputDBUserName>未设置.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		return para;
	}

	/**
	 * 获取数据库密码
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	private String getParamInputDBPasswd(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("inputDBPasswd");
		if (null == para || para.trim().length() <= 0) {
			String meg = "数据库密码<inputDBPasswd>未设置.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		return para;
	}

	/**
	 * 获取查询sql语句
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	private String getParamInputQuery(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("inputQuery");
		if (null == para || para.trim().length() <= 0) {
			return "";
		}

		return para;
	}

	/**
	 * 查询记录总数sql
	 * 
	 * @param paraMap
	 * @return
	 */
	private String getParamInputCountQuery(Map<String, String> paraMap) {
		String para = paraMap.get("inputCountQuery");
		if (null == para || para.trim().length() <= 0) {
			return "";
		}

		return para;
	}

	/**
	 * 获取查询数据缓冲大小
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	private int getParamInputQueryfetchSize(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("queryfetchSize");
		if (null == para || para.trim().length() <= 0) {
			LOG.warn("查询数据缓冲大小<queryfetchSize>未设置，默认为 100条数据.");
			return 128;
		}

		int size = 0;
		try {
			size = Integer.valueOf(para);
		} catch (Exception e) {
			String meg = "查询数据缓冲大小<queryfetchSize>数据异常, 必须大于0";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		if (size <= 0) {
			String meg = "查询数据缓冲大小<queryfetchSize>数据异常, 必须大于0";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		return size;
	}

	/**
	 * 获取源数据表名
	 * 
	 * @param paraMap
	 * @return
	 * @throws Exception
	 */
	private String getParamInputTableName(Map<String, String> paraMap) throws Exception {
		String para = paraMap.get("inputTableName");
		if (null == para || para.trim().length() <= 0) {
			return "";
		}

		return para;
	}

	/**
	 * 获取查询条件
	 * 
	 * @param paraMap
	 * @return
	 */
	private String getparamInputConditions(Map<String, String> paraMap) {
		String para = paraMap.get("inputConditions");
		if (null == para || para.trim().length() <= 0) {
			return "";
		}

		return para;
	}

	/**
	 * 获取绑定查询语句
	 * 
	 * @param paraMap
	 * @return
	 */
	private String getParamInputBoundingQuery(Map<String, String> paraMap) {
		String para = paraMap.get("inputBoundingQuery");
		if (null == para || para.trim().length() <= 0) {
			return "";
		}

		return para;
	}

	/**
	 * 获取排序条件
	 * 
	 * @param paraMap
	 * @return
	 */
	private String getParamInputOrderByColumn(Map<String, String> paraMap) {
		String para = paraMap.get("inputOrderByColumn");
		if (null == para || para.trim().length() <= 0) {
			return "";
		}

		return para;
	}

	protected static Map<String, String> testMRParamter() {
		// 系统参数
		String resource = "hbase-default.xml,hbase-site.xml";// 以逗号隔开
		String jobName = "M-R DB to File(orcale)";
		String jobPriority = "HIGH";
		String hdfsBaseUrl = "hdfs://hadoop01:9000";
		String jobTrackerHost = "hadoop01:9001";
		String dirInput = "/wanghao/isomerism";
		String dirOutput = "/wanghao/isomerism-result1";

		// 输入
		String isCombinerClass = "false"; // map是否合
		// mysql
		String inputDBDriverClass = "org.gjt.mm.mysql.Driver";
		String inputDBUrl = "jdbc:mysql://133.37.251.207:3306/mrtest";
		String inputDBUserName = "hive";
		String inputDBPasswd = "hive";
		// oracle
		// String inputDBDriverClass = "oracle.jdbc.driver.OracleDriver";
		// String inputDBUrl = "jdbc:oracle:thin:@133.37.251.241:1521:ora10";
		// String inputDBUserName = "meta";
		// String inputDBPasswd = "meta";

		String inputDBSplitType = "2";
		// inputDBSplitType=1：
		// A.参数设置(设置：inputQuery,inputCountQuery)
		// String inputQuery =
		// "SELECT sid, sprice, mprice, rowcounts FROM t_mrtestresult"; //
		// 必选,不能有分号
		// String inputCountQuery = "SELECT COUNT(*) FROM t_mrtestresult"; //
		// 必选,不能有分号
		// String inputFieldNames = "sid,sprice,mprice,rowcounts"; //
		// 可选,（若未设置,默认为表的所有字段, 并且必须设置inputTableName）
		// String inputTableName = "t_mrtestresult";
		// String inputConditions = "";
		// String inputBoundingQuery = "";
		// String inputOrderByColumn = "";

		// B.参数设置(设置：inputQuery,inputTableName)
		// String inputQuery =
		// "SELECT sid, sprice, mprice, rowcounts FROM t_mrtestresult where sprice>50";
		// // 必选(有条件必须设置inputConditions)
		// String inputFieldNames = "sid,sprice,mprice,rowcounts"; // 可选
		// String inputTableName = "t_mrtestresult"; // 必选
		// String inputConditions = "sprice>50"; // 可选
		// String inputCountQuery = "";
		// String inputBoundingQuery = "";
		// String inputOrderByColumn = "";

		// c.参数设置(设置：inputCountQuery,inputTableName)
		// String inputCountQuery = "SELECT COUNT(*) FROM t_mrtestresult"; // 必选
		// String inputFieldNames = "sid,sprice,mprice,rowcounts"; //
		// 可选(若不设置,默认为表的所有字段)
		// String inputTableName = "t_mrtestresult"; // 必选
		// String inputConditions = "";// 可选
		// String inputOrderByColumn = "";// 可选
		// String inputQuery = "";
		// String inputBoundingQuery = "";

		// d.参数设置(设置：inputTableName)
		// String inputTableName = "t_mrtestresult"; // 必选
		// String inputFieldNames = "sid,sprice,mprice,rowcounts"; //
		// 可选(若不设置,默认为表的所有字段)
		// String inputConditions = "sprice>30";// 可选
		// String inputOrderByColumn = "sid desc";// 可选
		// String inputQuery = "";
		// String inputCountQuery = "";
		// String inputBoundingQuery = "";

		// inputDBSplitType=2：
		// A.参数设置(必填：inputQuery, inputBoundingQuery, inputOrderByColumn,
		// 可选：inputTableName,inputFieldNames)
		// String inputQuery =
		// "SELECT sid, sprice, mprice, rowcounts FROM t_mrtestresult where $CONDITIONS";
		// // 必选,必须包含where $CONDITIONS"
		// String inputBoundingQuery =
		// "select min(sid),max(sid) from t_mrtestresult "; // 必选
		// String inputFieldNames = ""; // 可选, 若未设置, 默认为inputQuery中的字段
		// String inputTableName = "t_mrtestresult";// 可选
		// String inputConditions = "";
		// String inputOrderByColumn = " sid ";//必选, 限制一个列名
		// String inputCountQuery =null;

		// b.参数设置(必填：inputQuery, inputOrderByColumn,
		// 可选：inputTableName,inputFieldNames,inputConditions)
		// String inputQuery =
		// "SELECT sid, sprice, mprice, rowcounts FROM t_mrtestresult where $CONDITIONS";
		// // 必选,必须包含where $CONDITIONS"
		// String inputBoundingQuery = "";
		// String inputFieldNames = ""; // 可选, 若未设置, 默认为inputQuery中的字段
		// String inputTableName = "t_mrtestresult";// 可选
		// String inputConditions = ""; // 可选,
		// 若在inputQuery有除了$CONDITIONS之外的查询语句，那inputConditions必须和其一致
		// String inputOrderByColumn = " sid ";//必选, 限制一个列名
		// String inputCountQuery = null;

		// c.参数设置(必填：inputBoundingQuery, inputTableName, inputOrderByColumn,
		// 可选:inputFieldNames,inputConditions)
		// String inputQuery = "";
		// String inputBoundingQuery =
		// "select min(sid),max(sid) from t_mrtestresult ";
		// String inputFieldNames = ""; // 可选, 若未设置, 默认为inputQuery中的字段
		// String inputTableName = "t_mrtestresult";// 可选
		// String inputConditions = ""; // 可选,
		// 若在inputBoundingQuery有where的查询语句，那inputConditions必须和其一致
		// String inputOrderByColumn = " sid ";//必选, 限制一个列名
		// String inputCountQuery = null;

		// d.参数设置(必填： inputTableName, inputOrderByColumn,
		// 可选:inputFieldNames,inputConditions)
		String inputQuery = "";
		String inputBoundingQuery = "";
		String inputFieldNames = ""; // 可选, 若未设置, 默认为inputQuery中的字段
		String inputTableName = "t_mrtestresult";// 可选
		String inputConditions = ""; // 可选,
		// 若在inputBoundingQuery有where的查询语句，那inputConditions必须和其一致
		String inputOrderByColumn = " sid ";// 必选, 限制一个列名
		String inputCountQuery = null;

		// 输出
		String targetFilePath = "/wanghao/isomerism-DBToFile";
		String targetFieldNames = "id,minprice,maxprice,message,count";
		String outputFileFieldSplitChars = ",";
		String outputFileRowsSplitChars = "\n";
		String targetCompress = "false";
		String targetCompressCodec = HDFSUtils.CompressCodec.GzipCodec.toString();

		// 任务数
		String numMapTasks = "2";
		String numReduceTasks = "1";

		// 目标字段(targFieldNames):源字段(srcFieldNames):统计源字段的方法
		String groupFieldMethod = "id:sid:NONE,minprice:sprice:MIN,maxprice:mprice:MAX,message:rowcounts:NONE,count:sid:COUNT";

		Map<String, String> paraMap = new HashMap<String, String>();
		// 设置系统配置
		paraMap.put("resource", resource);
		paraMap.put("jobName", jobName);
		paraMap.put("jobPriority", jobPriority);
		paraMap.put("hdfsBaseUrl", hdfsBaseUrl);
		paraMap.put("jobTrackerHost", jobTrackerHost);
		paraMap.put("dirInput", dirInput);
		paraMap.put("dirOutput", dirOutput);

		// 设置输入
		paraMap.put("inputDBDriverClass", inputDBDriverClass);
		paraMap.put("inputDBUrl", inputDBUrl);
		paraMap.put("inputDBUserName", inputDBUserName);
		paraMap.put("inputDBPasswd", inputDBPasswd);
		paraMap.put("inputDBSplitType", inputDBSplitType);

		paraMap.put("inputQuery", inputQuery);
		paraMap.put("inputCountQuery", inputCountQuery);
		paraMap.put("inputTableName", inputTableName);
		paraMap.put("inputFieldNames", inputFieldNames);
		paraMap.put("inputConditions", inputConditions);
		paraMap.put("inputBoundingQuery", inputBoundingQuery);
		paraMap.put("inputOrderByColumn", inputOrderByColumn);
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

	protected static Map<String, String> testNoReduceParamter() {
		// 系统参数
		String resource = "hbase-default.xml,hbase-site.xml";// 以逗号隔开
		String jobName = "M-R DB to File(orcale)";
		String jobPriority = "HIGH";
		String hdfsBaseUrl = "hdfs://hadoop01:9000";
		String jobTrackerHost = "hadoop01:9001";
		String dirInput = "/wanghao/isomerism";
		String dirOutput = "/wanghao/isomerism-result1";

		// 输入
		String isCombinerClass = "false"; // map是否合
		// mysql
		// String inputDBDriverClass = "org.gjt.mm.mysql.Driver";
		// String inputDBUrl = "jdbc:mysql://133.37.251.207:3306/mrtest";
		// String inputDBUserName = "hive";
		// String inputDBPasswd = "hive";
		// oracle
		String inputDBDriverClass = "oracle.jdbc.driver.OracleDriver";
		String inputDBUrl = "jdbc:oracle:thin:@133.37.251.241:1521:ora10";
		String inputDBUserName = "meta";
		String inputDBPasswd = "meta";

		// inputDBSplitType=1：
		String inputDBSplitType = "2";
		// A.参数设置(设置：inputQuery,inputCountQuery)
		// String inputQuery =
		// "SELECT sid, sprice, mprice, rowcounts FROM t_mrtestresult"; //
		// 必选,不能有分号
		// String inputCountQuery = "SELECT COUNT(*) FROM t_mrtestresult"; //
		// 必选,不能有分号
		// String inputFieldNames = "sid,sprice,mprice,rowcounts"; //
		// 可选,（若未设置,默认为表的所有字段, 并且必须设置inputTableName）
		// String inputTableName = "t_mrtestresult";
		// String inputConditions = "";
		// String inputBoundingQuery = "";
		// String inputOrderByColumn = "";

		// B.参数设置(设置：inputQuery,inputTableName)
		// String inputQuery =
		// "SELECT sid, sprice, mprice, rowcounts FROM t_mrtestresult where sprice>50";
		// // 必选(有条件必须设置inputConditions)
		// String inputFieldNames = "sid,sprice,mprice,rowcounts"; // 可选
		// String inputTableName = "t_mrtestresult"; // 必选
		// String inputConditions = "sprice>50"; // 可选
		// String inputCountQuery = "";
		// String inputBoundingQuery = "";
		// String inputOrderByColumn = "";

		// c.参数设置(设置：inputCountQuery,inputTableName)
		// String inputCountQuery = "SELECT COUNT(*) FROM t_mrtestresult"; // 必选
		// String inputFieldNames = "sid,sprice,mprice,rowcounts"; //
		// 可选(若不设置,默认为表的所有字段)
		// String inputTableName = "t_mrtestresult"; // 必选
		// String inputConditions = "";// 可选
		// String inputOrderByColumn = "";// 可选
		// String inputQuery = "";
		// String inputBoundingQuery = "";

		// d.参数设置(设置：inputTableName)
		// String inputTableName = "t_mrtestresult"; // 必选
		// String inputFieldNames = "sid,sprice,mprice,rowcounts"; //
		// 可选(若不设置,默认为表的所有字段)
		// String inputConditions = "sprice>30";// 可选
		// String inputOrderByColumn = "sid desc";// 可选
		// String inputQuery = "";
		// String inputCountQuery = "";
		// String inputBoundingQuery = "";

		// inputDBSplitType=2：
		// A.参数设置(必填：inputQuery, inputBoundingQuery, inputOrderByColumn,
		// 可选：inputTableName,inputFieldNames)
		// String inputQuery =
		// "SELECT sid, sprice, mprice, rowcounts FROM t_mrtestresult where $CONDITIONS";
		// // 必选,必须包含where $CONDITIONS"
		// String inputBoundingQuery =
		// "select min(sid),max(sid) from t_mrtestresult "; // 必选
		// String inputFieldNames = ""; // 可选, 若未设置, 默认为inputQuery中的字段
		// String inputTableName = "t_mrtestresult";// 可选
		// String inputConditions = "";
		// String inputOrderByColumn = " sid ";//必选, 限制一个列名
		// String inputCountQuery =null;

		// b.参数设置(必填：inputQuery, inputTableName, inputOrderByColumn,
		// 可选：inputBoundingQuery,inputFieldNames,inputConditions)
		String inputQuery = "SELECT sid, sprice, mprice, rowcounts FROM t_mrtestresult where $CONDITIONS"; // 必选,必须包含where
		// $CONDITIONS"
		String inputBoundingQuery = "";
		String inputFieldNames = ""; // 可选, 若未设置, 默认为inputQuery中的字段
		String inputTableName = "t_mrtestresult";// 可选
		String inputConditions = ""; // 可选,
		// 若在inputQuery有除了$CONDITIONS之外的查询语句，那inputConditions必须和其一致
		String inputOrderByColumn = " sid ";// 必选, 限制一个列名
		String inputCountQuery = null;

		// c.参数设置(必填：inputBoundingQuery, inputTableName, inputOrderByColumn,
		// 可选:inputQuery, inputFieldNames,inputConditions)
		// String inputQuery = "";
		// String inputBoundingQuery =
		// "select min(sid),max(sid) from t_mrtestresult ";
		// String inputFieldNames = ""; // 可选, 若未设置, 默认为inputQuery中的字段
		// String inputTableName = "t_mrtestresult";// 可选
		// String inputConditions = ""; // 可选,
		// 若在inputBoundingQuery有where的查询语句，那inputConditions必须和其一致
		// String inputOrderByColumn = " sid ";//必选, 限制一个列名
		// String inputCountQuery = null;

		// d.参数设置(必填： inputTableName, inputOrderByColumn,
		// 可选:inputQuery,inputBoundingQuery,inputFieldNames,inputConditions)
		// String inputQuery = "";
		// String inputBoundingQuery = "";
		// String inputFieldNames = ""; // 可选, 若未设置, 默认为inputQuery中的字段
		// String inputTableName = "t_mrtestresult";// 可选
		// String inputConditions = ""; // 可选,
		// 若在inputBoundingQuery有where的查询语句，那inputConditions必须和其一致
		// String inputOrderByColumn = " sid ";//必选, 限制一个列名
		// String inputCountQuery = null;

		// 输出
		String targetFilePath = "/wanghao/isomerism-DBToFile";
		String targetFieldNames = "id,minprice,maxprice,message";
		String outputFileFieldSplitChars = ",";
		String outputFileRowsSplitChars = "\n";
		String targetCompress = "false";
		String targetCompressCodec = HDFSUtils.CompressCodec.GzipCodec.toString();

		// 任务数
		String numMapTasks = "2";
		String numReduceTasks = "1";

		// 目标字段(targFieldNames):源字段(srcFieldNames):统计源字段的方法
		String groupFieldMethod = "id:sid:NONE,minprice:sprice:MIN,maxprice:mprice:MAX,message:rowcounts:NONE";

		Map<String, String> paraMap = new HashMap<String, String>();
		// 设置系统配置
		paraMap.put("resource", resource);
		paraMap.put("jobName", jobName);
		paraMap.put("jobPriority", jobPriority);
		paraMap.put("hdfsBaseUrl", hdfsBaseUrl);
		paraMap.put("jobTrackerHost", jobTrackerHost);
		paraMap.put("dirInput", dirInput);
		paraMap.put("dirOutput", dirOutput);

		// 设置输入
		paraMap.put("inputDBDriverClass", inputDBDriverClass);
		paraMap.put("inputDBUrl", inputDBUrl);
		paraMap.put("inputDBUserName", inputDBUserName);
		paraMap.put("inputDBPasswd", inputDBPasswd);
		paraMap.put("inputDBSplitType", inputDBSplitType);

		paraMap.put("inputQuery", inputQuery);
		paraMap.put("inputCountQuery", inputCountQuery);
		paraMap.put("inputTableName", inputTableName);
		paraMap.put("inputFieldNames", inputFieldNames);
		paraMap.put("inputConditions", inputConditions);
		paraMap.put("inputBoundingQuery", inputBoundingQuery);
		paraMap.put("inputOrderByColumn", inputOrderByColumn);
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
	 * 入口函数
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		MRDBToFileService s = new MRDBToFileService();
		s.run(testMRParamter());
		// s.run(testNoReduceParamter());
	}
}