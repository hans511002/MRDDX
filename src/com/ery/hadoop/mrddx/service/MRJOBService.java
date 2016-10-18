package com.ery.hadoop.mrddx.service;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.zookeeper.KeeperException;

import com.ery.hadoop.mrddx.DBGroupReducer;
import com.ery.hadoop.mrddx.DataTypeConstant;
import com.ery.hadoop.mrddx.IHandleFormat;
import com.ery.hadoop.mrddx.IMROutputFormatEnd;
import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.hbase.HbaseConfiguration;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.remote.FTPJob;
import com.ery.hadoop.mrddx.senior.MRSeniorUtil;
import com.ery.hadoop.mrddx.util.HDFSUtils;
import com.ery.hadoop.mrddx.util.StringUtil;
import com.ery.hadoop.mrddx.util.SystemUtil;
import com.ery.hadoop.mrddx.zk.IMRZooKeeper;
import com.ery.hadoop.mrddx.zk.MRFileZookeeper;
import com.ery.hadoop.mrddx.zk.MRZooKeeper;
import com.ery.hadoop.mrddx.zk.ZKUtil;
import com.ery.base.support.log4j.LogUtils;

/**
 * 
 * 提供给客户调用的服务类
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-2-5
 * @version v1.0
 */
public class MRJOBService {
	// 日志对象
	public static final Log LOG = LogFactory.getLog(MRJOBService.class);

	// job下zk的根路径
	private String zkRootPath;

	// zk的连接地址
	private String zkAddress;

	// job的zk节点
	private String jobZKNodePath;

	// zk节点路径列表
	private List<String> lstZKNodePath = new ArrayList<String>();

	// zk对象列表
	private List<MRZooKeeper> lstZK = new ArrayList<MRZooKeeper>();

	// 是否存在处理数据的任务(true:存在)
	private boolean doJob;

	// 队列名称
	private String queueName;

	/**
	 * 运行JOB
	 * 
	 * @param paramMap
	 *            job运行需要的参数
	 * @param conf
	 *            配置对象
	 * @throws Exception
	 *             异常
	 */
	public void run(Map<String, String> paramMap, Job job) throws Exception {
		Configuration conf = job.getConfiguration();
		MRConfiguration mrconf = new MRConfiguration(conf);
		mrconf.decodeTempMapredJobPriority();
		// 初始化日志参数
		long logId = StringUtil.getTime(System.currentTimeMillis());
		long jobId = mrconf.getSysJobId();

		// 日志
		mrconf.setJobLogId(logId); // 设置JOB的日志Id
		MRLog.getInstance().setConf(mrconf);

		// 设置系统参数
		String pSysResource[] = mrconf.getResource();
		for (int i = 0; i < pSysResource.length; i++) {
			conf.addResource(pSysResource[i]);
		}

		// 获取ftp操作的类型
		int fileDownftptype = mrconf.getFileDownOrDoFTPType();
		int fileUpFtpType = mrconf.getFileUPFTPType();

		// 校验系统参数(与业务无关)
		this.validateSysParameter(conf, mrconf);
		this.validateDataDo(job, mrconf);// 验证数据处理的参数
		this.validateZKParameter(mrconf);
		this.validateLogParameter(mrconf);

		// 验证数据处理与业务相关的系统参数
		if (MRConfiguration.INTERNAL_SYS_FILE_DOWN_OR_DO_FTPTYPE_DOWN_AND_DO.equals(String.valueOf(fileDownftptype)) ||
				MRConfiguration.INTERNAL_SYS_FILE_DOWN_OR_DO_FTPTYPE_DO.equals(String.valueOf(fileDownftptype))) {
			this.validateDataDoParameter(conf, mrconf);// 验证与业务相关的系统参数
			this.doJob = true;
		}

		// 设置需要通过zk节点来监控job运行
		this.zkmonitor(mrconf);

		// 只验证JOB参数，非运行JOB
		if (mrconf.isTestJob()) {
			this.close();// 关闭资源
			MRLog.systemOut("TEST SUCCESS");
			return;
		}
		this.queueName = mrconf.getMapredJobQueueName();
		// 运行任务
		switch (fileDownftptype) {
		case 0: // 只下载(对输入是文件有效)
			MRLog.systemOut("only down. start");
			this.downLoadToHDFSJOB(job, mrconf);
			MRLog.systemOut("only down. end");
			break;
		case 1: // 下载(对输入是文件有效)并处理
			MRLog.getInstance().addJobRun(logId, jobId, StringUtil.getMonthNo(new Date()),
					StringUtil.getDateNo(new Date()), new Date(), 0, mrconf.getRunJobCmd(), this.queueName);
			MRLog.systemOut("down and do. start");
			MRLog.systemOut("<job_log_id>" + logId + "</job_log_id>");
			this.downLoadToHDFSJOB(job, mrconf, logId);
			MRLog.systemOut("down and do. end");
			break;
		case 2: // 只处理
			MRLog.getInstance().addJobRun(logId, jobId, StringUtil.getMonthNo(new Date()),
					StringUtil.getDateNo(new Date()), new Date(), 0, mrconf.getRunJobCmd(), this.queueName);
			MRLog.systemOut("only do. start");
			MRLog.systemOut("<job_log_id>" + logId + "</job_log_id>");
			this.runDataDo(job, mrconf, logId);
			MRLog.systemOut("only do. end");

			break;
		}

		// this.runDataDoMr(job, mrconf, logId);

		switch (fileUpFtpType) {// 只上传(对输出结果为文件有效)
		case 0:
			MRLog.systemOut("only upload. start");
			this.uploadDataJOB(job, mrconf, logId);
			MRLog.systemOut("only upload. end");
			break;
		default:
			break;
		}
		// 判断是否分区写文件模式，是否需要写导入分区库命令
		Class<?> outClz = job.getOutputFormatClass();
		LogUtils.info("check processend OutputFormatClass " + outClz.getName() +
				" is implements interface IMROutputFormatEnd ： " + IMROutputFormatEnd.class.isAssignableFrom(outClz));
		if (IMROutputFormatEnd.class.isAssignableFrom(outClz)) {
			try {
				LogUtils.info("need processend mothed");
				Object obj = outClz.newInstance();
				IMROutputFormatEnd pend = (IMROutputFormatEnd) obj;
				pend.processEnd(conf);
				LogUtils.info("processend success");
			} catch (Exception e) {
				LogUtils.warn("mr processend failed", e);
			}
		}
	}

	private void validateDataDo(Job conf, MRConfiguration mrconf) throws ClassNotFoundException, Exception,
			InstantiationException, IllegalAccessException {
		// 获取输出输入Format
		List<Class<?>> inputFormatClass = mrconf.getInputFormatClass();
		List<Class<?>> outputFormatClass = mrconf.getOutputFormatClass();
		if (null == inputFormatClass || null == outputFormatClass) {
			throw new Exception("inputFormatClass or outputFormatClass不存在");
		}

		for (Class<?> clazz : inputFormatClass) {
			Object inputFormat = clazz.newInstance();
			if (!(inputFormat instanceof IHandleFormat)) {
				throw new Exception("InputFormatClass must implement IHandleFormat!");
			}
			((IHandleFormat) inputFormat).handle(conf);
		}

		for (Class<?> clazz : outputFormatClass) {
			Object outputFormat = clazz.newInstance();
			if (!(outputFormat instanceof IHandleFormat)) {
				throw new Exception("outputFormatClass must implement IHandleFormat!");
			}
			((IHandleFormat) outputFormat).handle(conf);
		}
	}

	/**
	 * 只运行数据处理
	 * 
	 * @param conf
	 * @param mrconf
	 * @param logId
	 * @throws IOException
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws Exception
	 */
	private void runDataDo(Job job, MRConfiguration mrconf, long logId) throws IOException, KeeperException,
			InterruptedException, Exception {
		boolean flag = false;
		boolean isHFILEToHBase = this.isHFILEToHBase(job.getConfiguration());
		String hadoopHome = mrconf.getSysHadoopHome();
		String hbaseHome = mrconf.getSysHBaseHome();
		String pSysDirOutput = "";
		HbaseConfiguration hConf = new HbaseConfiguration(job.getConfiguration(), HbaseConfiguration.FLAG_HBASE_OUTPUT);
		String tableName = hConf.getOutputHBaseTableName();
		String hbasejarName = hConf.getOutputHBaseJarName();
		// 运行job
		try {
			job.waitForCompletion(true);
			// job.submit();
			// 移动文件
			pSysDirOutput = mrconf.getMapredOutputDir();
			String pOutputFilePath = mrconf.getOutputTargetPath();
			if (null != pOutputFilePath && pOutputFilePath.trim().length() > 0) {
				// 将文件从临时目录拷贝到目标目录下
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
				String prefix = sdf.format(new Date());
				HDFSUtils.move(job.getConfiguration(), pSysDirOutput + "/part-*", pOutputFilePath, prefix);
			}
			flag = true;
		} catch (Exception e) {
			MRLog.errorException(LOG, e);
			throw new Exception(e);
		} finally {
			// 关闭资源
			this.close();
		}

		// 执行blukload写入hbase的命令
		if (isHFILEToHBase) {
			if (!(null == hadoopHome || hadoopHome.trim().length() <= 0 || null == hbaseHome || hbaseHome.trim()
					.length() <= 0)) {
				String command = hadoopHome + "/bin/hadoop jar " + hbaseHome + "/" + hbasejarName +
						" completebulkload " + pSysDirOutput + " " + tableName;
				MRLog.systemOut("Run Blukload Command:" + command);
				SystemUtil.exec(command);
			} else {
				MRLog.systemOut("Run Blukload Command error, HADOOP_HOME=" + hadoopHome + " HBASE_HOME=" + hbaseHome);
			}
		}
		// 日志
		if (flag) {
			MRLog.getInstance().updateJobRun(logId, new Date(), 1, mrconf.getJOBLogRowcord(), "sucess");
			MRLog.systemOut("<handle_result>success</handle_result>");
		} else {
			MRLog.getInstance().updateJobRun(logId, new Date(), 2, mrconf.getJOBLogRowcord(), "failed");
			MRLog.systemOut("<handle_result>fail</handle_result>");
		}
	}

	/**
	 * 若数据来源为FTP、SFTP、本地，则需要预先将数据写入到文件系统中
	 * 
	 * @param logId
	 * @param mrconf
	 * 
	 * @param mrconf
	 *            runDownLoadToHDFS
	 * @throws Exception
	 */
	private void downLoadToHDFSJOB(Job conf, MRConfiguration mrconf) throws Exception {
		try {
			FTPJob ftpJob = new FTPJob();
			ftpJob.runDown(conf, mrconf);
		} catch (Exception e) {
			// 日志
			MRLog.errorException(LOG, e);
			throw new Exception(e);
		} finally {
			// 关闭资源
			this.close();
		}
	}

	/**
	 * 若数据来源为FTP、SFTP、本地，则需要预先将数据写入到文件系统中
	 * 
	 * @param logId
	 * @param mrconf
	 * 
	 * @param mrconf
	 *            runDownLoadToHDFS
	 * @throws Exception
	 */
	private void downLoadToHDFSJOB(Job conf, MRConfiguration mrconf, long logId) throws Exception {
		boolean flag = false;
		try {
			FTPJob ftpJob = new FTPJob();
			ftpJob.runDown(conf, mrconf);
			// 移动文件
			String pSysDirOutput = mrconf.getMapredOutputDir();
			String pOutputFilePath = mrconf.getOutputTargetPath();
			if (null != pOutputFilePath && pOutputFilePath.trim().length() > 0) {
				// 将文件从临时目录拷贝到目标目录下
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
				String prefix = sdf.format(new Date());
				HDFSUtils.move(conf.getConfiguration(), pSysDirOutput + "/part-*", pOutputFilePath, prefix);
			}
			flag = true;
		} catch (Exception e) {
			// 日志
			MRLog.errorException(LOG, e);
			throw new Exception(e);
		} finally {
			// 关闭资源
			this.close();
		}

		// 日志
		if (flag) {
			MRLog.getInstance().updateJobRun(logId, new Date(), 1, mrconf.getJOBLogRowcord(), "sucess");
			MRLog.systemOut("<handle_result>success</handle_result>");
		} else {
			MRLog.getInstance().updateJobRun(logId, new Date(), 2, mrconf.getJOBLogRowcord(), "failed");
			MRLog.systemOut("<handle_result>fail</handle_result>");
		}
	}

	/**
	 * 若数据目的为FTP、SFTP、本地，则需要预先将文件系统中的数据输出到远程中
	 * 
	 * @param logId
	 * @param mrconf
	 * 
	 * @param mrconf
	 * @throws Exception
	 */
	public void uploadDataJOB(Job conf, MRConfiguration mrconf, long logId) throws Exception {
		try {
			FTPJob ftpJob = new FTPJob();
			ftpJob.runUp(conf);
		} catch (Exception e) {
			// 日志
			MRLog.errorException(LOG, e);
			throw new Exception(e);
		}
	}

	/**
	 * 设置监控job运行
	 * 
	 * @param mrconf
	 *            配置对象
	 * @throws IOException
	 *             IO异常
	 * @throws KeeperException
	 *             zk异常
	 * @throws InterruptedException
	 *             线程打断异常
	 */
	private void zkmonitor(MRConfiguration mrconf) throws IOException, KeeperException, InterruptedException {
		boolean inputMapEnd = mrconf.getInputMapEnd();// 是否map直接输出结果
		String jobName = HDFSUtils.getJobName(mrconf);
		this.zkRootPath = mrconf.getZKRootPath();
		this.zkAddress = mrconf.getZKAddress();
		this.jobZKNodePath = this.zkRootPath + "/" + jobName;

		if (inputMapEnd && mrconf.isZKMonitor()) {
			// 添加map zk临时节点
			String mapperZKNodePath = this.jobZKNodePath + "/mapper";
			MRZooKeeper mapperZK = new MRZooKeeper(this.zkAddress, mapperZKNodePath);
			mapperZK.apendMapperNode(mapperZKNodePath, "");
			mrconf.setZKMapperPath(mapperZKNodePath);
			this.lstZK.add(mapperZK);
			this.lstZKNodePath.add(mapperZKNodePath);
		}

		if (!inputMapEnd && mrconf.isZKMonitor()) {
			// 添加reduce zk临时节点
			String reduceZKNodePath = this.jobZKNodePath + "/reduce";
			MRZooKeeper reduceZK = new MRZooKeeper(this.zkAddress, reduceZKNodePath);
			reduceZK.apendReduceNode(reduceZKNodePath, "");
			mrconf.setZKReducePath(reduceZKNodePath);
			this.lstZK.add(reduceZK);
			this.lstZKNodePath.add(reduceZKNodePath);
		}

		// 设置监听采集节点
		String fileDownftptype = String.valueOf(mrconf.getFileDownOrDoFTPType());
		String fileUpFtpType = String.valueOf(mrconf.getFileUPFTPType());
		if (MRConfiguration.INTERNAL_SYS_FILE_DOWN_OR_DO_FTPTYPE_DOWN.equals(fileDownftptype) ||
				MRConfiguration.INTERNAL_SYS_FILE_DOWN_OR_DO_FTPTYPE_DOWN_AND_DO.equals(fileDownftptype) ||
				MRConfiguration.INTERNAL_SYS_FILE_UP_FTPTYPE_UP.equals(fileUpFtpType)) {
			// 添加ftp zk临时节点
			String ftpDownZKNodePath = this.jobZKNodePath + "/ftp";
			MRZooKeeper ftpDownZK = new MRZooKeeper(this.zkAddress, ftpDownZKNodePath);
			ftpDownZK.apendFTPNode(ftpDownZKNodePath, "");
			mrconf.setZKFTPPath(ftpDownZKNodePath);
			this.lstZK.add(ftpDownZK);
			this.lstZKNodePath.add(ftpDownZKNodePath);
		}

		// 添加处理文件，创建JOBID ZK节点, 并且加锁，防止同时处理相同的任务
		// 只对输入为文件操作
		if (!(MRConfiguration.INTERNAL_SYS_FILE_DOWN_OR_DO_FTPTYPE_DOWN_AND_DO.equals(fileDownftptype) || MRConfiguration.INTERNAL_SYS_FILE_DOWN_OR_DO_FTPTYPE_DO
				.equals(fileDownftptype))) {
			return;
		}

		String[] inputType = mrconf.getInputFormatType();
		for (int i = 0; i < inputType.length; i++) {
			if (DataTypeConstant.FILETEXT.equalsIgnoreCase(inputType[i]) ||
					DataTypeConstant.FILESEQUENCE.equalsIgnoreCase(inputType[i]) ||
					DataTypeConstant.FILERCFILE.equalsIgnoreCase(inputType[i])) {
				MRFileZookeeper mrFileZK = new MRFileZookeeper(mrconf.getConf());
				mrFileZK.init();
				mrFileZK.lock();
			}
		}
	}

	/**
	 * 验证与日志相关的参数
	 * 
	 * @param mrconf
	 *            配置对象
	 * @throws Exception
	 *             异常
	 */
	private void validateLogParameter(MRConfiguration mrconf) throws Exception {
		String url = mrconf.getLogJDBCUrl();
		if (null == url || url.trim().length() <= 0) {
			String meg = "存储日志信息数据库地址<" + MRConfiguration.JOB_SYS_LOG_JDBC_URL + ">未配置.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		String user = mrconf.getLogJDBCUser();
		if (null == user) {
			String meg = "存储日志信息数据库用户名<" + MRConfiguration.JOB_SYS_LOG_JDBC_USER + ">未配置.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		String pwd = mrconf.getLogJDBCPwd();
		if (null == pwd) {
			String meg = "存储日志信息数据库密码<" + MRConfiguration.JOB_SYS_LOG_JDBC_PWD + ">未配置.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}
	}

	/**
	 * 验证zk系统参数
	 * 
	 * @param mrconf
	 *            配置对象
	 * @throws Exception
	 *             异常
	 */
	private void validateZKParameter(MRConfiguration mrconf) throws Exception {
		String address = mrconf.getZKAddress();
		if (null == address || address.trim().length() <= 0) {
			String meg = "zookeeper地址<" + MRConfiguration.SYS_ZK_ADDRESS + ">未配置.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}
	}

	/**
	 * 校验系统参数
	 * 
	 * @param conf
	 *            配置对象
	 * @param dbconf
	 *            配置对象
	 * @throws Exception
	 *             异常
	 */
	public void validateDataDoParameter(Configuration conf, MRConfiguration dbconf) throws Exception {
		// 源字段
		String[] inputFieldName = dbconf.getInputFieldNames();
		if (null == inputFieldName || inputFieldName.length <= 0) {
			String meg = "源字段<" + MRConfiguration.SYS_INPUT_FIELD_NAMES_PROPERTY + ">为空.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		// 目标字段
		String[] outputFieldName = dbconf.getOutputFieldNames();
		if (null == outputFieldName || outputFieldName.length <= 0) {
			String meg = "目标字段<" + MRConfiguration.SYS_OUTPUT_FIELD_NAMES_PROPERTY + ">为空.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		boolean isMapEnd = false;
		// 源字段与目标字段的对应关系
		Set<String> inputFieldSet = StringUtil.parseStringArrayToSet(inputFieldName);
		Set<String> outputFieldSet = StringUtil.parseStringArrayToSet(outputFieldName);

		// 获取目标字段:源字段:统计方法
		String[] relatedGroupField = dbconf.getRelatedGroupFieldMethod();
		if (null == relatedGroupField || relatedGroupField.length <= 0) {
			String meg = "目标字段:源字段:统计方法<" + MRConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL + ">未设置.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		boolean isExistGroupMethod = false; // 至少有一个group方法
		boolean isNotExistGroupMethod = false; // 没有group方法
		boolean isAllGroupMethodNONE = true;
		for (int i = 0; i < relatedGroupField.length; i++) {
			String per[] = relatedGroupField[i].split(":");
			switch (per.length) {
			case 2:
				// 校验输入字段和输出字段
				if (!outputFieldSet.contains(per[0])) {
					String msg = "目标字段" + per[0] + "不存在于输出字段 <" +
							MRConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL + ">";
					MRLog.error(LOG, msg);
					throw new RuntimeException(msg);
				}

				if (!inputFieldSet.contains(per[1])) {
					String msg = "源字段" + per[1] + "不存在输入字段<" + MRConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL +
							">";
					MRLog.error(LOG, msg);
					throw new RuntimeException(msg);
				}

				if (!isNotExistGroupMethod) {
					isNotExistGroupMethod = true;
				}

				break;
			case 3:
				// 校验输入字段和输出字段以及方法名称
				if (!outputFieldSet.contains(per[0])) {
					String msg = "目标字段" + per[0] + "不存在于输出字段 <" +
							MRConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL + ">";
					MRLog.error(LOG, msg);
					throw new RuntimeException(msg);
				}

				if (!inputFieldSet.contains(per[1])) {
					String msg = "源字段" + per[1] + "不存在输入字段<" + MRConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL +
							">";
					MRLog.error(LOG, msg);
					throw new RuntimeException(msg);
				}

				if (DBGroupReducer.GROUP_METHODS.get(per[2]) == null) {
					String msg = "统计方法" + per[2] + "不支持!";
					MRLog.error(LOG, msg);
					throw new RuntimeException(msg);
				}

				if (!isExistGroupMethod && DBGroupReducer.GROUP_METHODS.get(per[2]) != null) {
					isExistGroupMethod = true;
				}

				if (isAllGroupMethodNONE && DBGroupReducer.GROUP_METHODS.get(per[2]) != 0) {
					isAllGroupMethodNONE = false;
				}

				break;
			default:
				String meg = "目标字段:源字段:统计方法<" + MRConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL + ">配置错误.";
				MRLog.error(LOG, meg);
				throw new Exception(meg);
			}
		}

		// 统计方法异常
		if (isExistGroupMethod && isNotExistGroupMethod) {
			String msg = "配置错误,要么都存在统计方法,要么都不存在统计方法";
			MRLog.error(LOG, msg);
			throw new RuntimeException(msg);
		}

		// 验证用于直接输出的输出字段的值参数，其中输出字段是否包含在输出字段中
		Map<String, String> outColumnDefaultValue = StringUtil.decodeOutColumnDefaultValue(dbconf
				.getOutputColumnDefaultValue());// 获取直接输出的输出字段的默认值
		for (String columnName : outColumnDefaultValue.keySet()) {
			if (!outputFieldSet.contains(columnName)) {
				String meg = "设置输出字段默认值参数中的字段名：" + columnName + "不存在输出字段列表中，" +
						MRConfiguration.SYS_OUTPUT_COLUMN_DEFAULT_VALUE + ">配置错误.";
				MRLog.error(LOG, meg);
				throw new Exception(meg);
			}
		}

		// map直接输出判断
		if (!isExistGroupMethod || isAllGroupMethodNONE) {
			isMapEnd = true;
			dbconf.setNumReduceTasks(0);
		}

		// 只过滤，不入库的标示
		if (dbconf.getInputMapOnlyFilterNotStorage()) {
			isMapEnd = true;
			dbconf.setNumReduceTasks(0);
		}

		dbconf.setInputMapEnd(isMapEnd);

		// 验证在输入与输出字段关系中，目标字段一致，统计方法不同或者有重复的情况
		if (!isMapEnd) {
			Set<String> targetFileSet = new HashSet<String>();
			for (int i = 0; i < relatedGroupField.length; i++) {
				String per[] = relatedGroupField[i].split(":");
				if (targetFileSet.contains(per[0])) {
					String msg = "目标字段" + per[0] + "在<" + MRConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL +
							">存在重复!";
					MRLog.error(LOG, msg);
					throw new RuntimeException(msg);
				} else {
					targetFileSet.add(per[0]);
				}
			}
		}

		// 获取reduce数量(map直接输出，reduce数量必须为0)
		int reduceNumber = dbconf.getNumReduceTasks();
		if (isMapEnd && reduceNumber > 0) {
			String meg = "Map直接输出，reduce数量<" + MRConfiguration.MAPRED_REDUCE_TASKS + ">必须为0";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		if (!isMapEnd && reduceNumber <= 0) {
			String meg = "reduce数量<" + MRConfiguration.MAPRED_REDUCE_TASKS + ">必须大于0";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		// 验证高级配置
		String seniorConf = dbconf.getInputMapSeniorConf();
		if (null != seniorConf && seniorConf.length() > 0) {
			// 初始化Senior对象
			String oper[] = seniorConf.split(",");
			for (int i = 0; i < oper.length; i++) {
				String ch[] = oper[i].split(":");
				if (null == MRSeniorUtil.startOperation(oper[i])) {
					String msg = "高级使用配置有误, 操作类型:" + oper[i] + "不存在, 请查看使用使用说明书。";
					MRLog.error(LOG, msg);
					throw new Exception(msg);
				}

				if (null == ch[1] && ch[1].trim().length() <= 0) {
					String msg = "高级使用配置有误, 操作类型:" + ch[0] + "对应的表达式不能为空。";
					MRLog.error(LOG, msg);
					throw new Exception(msg);
				}
			}
		}
	}

	/**
	 * 校验系统参数
	 * 
	 * @param conf
	 *            配置对象
	 * @param dbconf
	 *            配置对象
	 * @throws Exception
	 *             异常
	 */
	public void validateSysParameter(Configuration conf, MRConfiguration dbconf) throws Exception {
		// job名称
		String jobName = dbconf.getJobName();
		if (null == jobName || jobName.trim().length() <= 0) {
			String meg = "job名称<" + MRConfiguration.MAPRED_JOB_NAME + ">未设置.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		// job优先级验证
		JobPriority priority = dbconf.getTempMapredJobPriority();
		if (null == priority) {
			String meg = "job优先级<JOB_PRIORITY>取值错误,应该为取值范围[1-5]:1-VERY_LOW,2-LOW,3-NORMAL,4-HIGH,5-VERY_HIGH.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		// hdfsBaseUrl文件系统地址
		String hdfsBaseUrl = dbconf.getFSDefaultName();
		if (null == hdfsBaseUrl || hdfsBaseUrl.trim().length() <= 0) {
			String meg = "文件系统地址<" + MRConfiguration.FS_DEFAULT_NAME + ">必须设置.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		// // jobhost
		// String mapredJobTracker = dbconf.getMapredJobTracker();
		// if (null == mapredJobTracker || mapredJobTracker.trim().length() <=
		// 0) {
		// String meg = "jobhost<" + MRConfiguration.MAPRED_JOB_TRACKER +
		// ">必须设置.";
		// MRLog.error(LOG, meg);
		// throw new Exception(meg);
		// }

		// 资源文件
		String[] resource = dbconf.getResource();
		if (null == resource || resource.length <= 0) {
			LOG.warn("资源文件<" + MRConfiguration.SYS_RESOURCE + ">未设置");
		}

		// 输入路径
		String dirInput = dbconf.getMapredInputDir();
		if (null == dirInput || dirInput.trim().length() <= 0) {
			String meg = "输入路径<" + MRConfiguration.MAPRED_INPUT_DIR + ">必须设置.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		// 获取临时输出路径
		String dirOutput = dbconf.getMapredOutputDir();
		if (null == dirOutput || dirOutput.trim().length() <= 0) {
			String path = HDFSUtils.getNoExistDir(conf, dbconf.getJobOutputRootPath() + "/" + dbconf.getSysJobId() +
					"_" + dbconf.getJobLogId());
			dbconf.setMapredOutputDir(path);
			MRLog.info(LOG, "输出路径为:" + path);
		}

		// task数量
		int mapNumber = dbconf.getNumMapTasks();
		if (mapNumber <= 0) {
			String meg = "map数量<" + MRConfiguration.MAPRED_MAP_TASKS + ">必须大于0";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		// 获取reduce数量
		int reduceNumber = dbconf.getNumReduceTasks();
		if (reduceNumber < 0) {
			String meg = "reduce数量<" + MRConfiguration.MAPRED_REDUCE_TASKS + ">必须等于0";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		// map直接输出
		if (mapNumber > 0 && reduceNumber <= 0) {
			dbconf.setInputMapEnd(true);
		}
	}

	/**
	 * 验证通过hfile入hbase
	 * 
	 * @param conf
	 * @return
	 * @throws ClassNotFoundException
	 * @throws Exception
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public boolean isHFILEToHBase(Configuration conf) throws ClassNotFoundException, Exception, InstantiationException,
			IllegalAccessException {
		// 获取输出输入Format
		String inputTypes[] = conf.getStrings(MRConfiguration.SYS_INPUT_FORMAT_TYPE, new String[0]);
		String outputTypes[] = conf.getStrings(MRConfiguration.SYS_OUTPUT_FORMAT_TYPE, new String[0]);
		boolean flag = false;
		// for (int i = 0; i < inputTypes.length; i++) {
		// if (DataTypeConstant.MRHFILE.equals(inputTypes[i])) {
		// flag = true;
		// break;
		// }
		// }
		// 只判断输出
		for (int i = 0; i < outputTypes.length; i++) {
			if (DataTypeConstant.MRHFILE.equals(outputTypes[i])) {
				flag = true && flag;
				break;
			}
		}

		return flag;
	}

	/**
	 * 关闭资源
	 */
	private void close() {
		String msg = "关闭zk节点异常.";
		for (MRZooKeeper zk : this.lstZK) {
			if (zk != null) {
				try {
					zk.destroy();
				} catch (InterruptedException e) {
					if (this.doJob) {
						MRLog.errorExceptionZK(LOG, msg, e);
					} else {
						MRLog.systemOut(msg);
					}
				}
			}
		}

		if (this.lstZKNodePath.size() <= 0) {
			return;
		}

		IMRZooKeeper mrzk = null;
		try {
			mrzk = new MRZooKeeper(this.zkAddress, null);
		} catch (IOException e) {
			if (this.doJob) {
				MRLog.errorExceptionZK(LOG, msg, e);
			} else {
				MRLog.systemOut(msg);
			}
		}

		if (null == mrzk) {
			return;
		}

		// 删除job下的子节点
		for (String nodePath : this.lstZKNodePath) {
			try {
				ZKUtil.deleteZKChildNode(mrzk, nodePath, LOG);
				ZKUtil.deleteZKNode(mrzk, nodePath, LOG);
			} catch (Exception e) {
				if (this.doJob) {
					MRLog.errorExceptionZK(LOG, msg, e);
				} else {
					MRLog.systemOut(msg);
				}
			}
		}

		// 删除job节点
		try {
			ZKUtil.deleteZKNode(mrzk, this.jobZKNodePath, LOG);
			mrzk.destroy();
		} catch (Exception e) {
			if (this.doJob) {
				MRLog.errorExceptionZK(LOG, msg, e);
			} else {
				MRLog.systemOut(msg);
			}
		}
	}
}
