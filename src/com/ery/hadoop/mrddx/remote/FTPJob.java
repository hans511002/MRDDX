package com.ery.hadoop.mrddx.remote;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.ery.hadoop.mrddx.DBGroupReducer;
import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.DBReducer;
import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.log.MRColDetailLog;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.StringUtil;

public class FTPJob {
	public static final String HDFS_HOST = "hdfsHost";
	public static final String JOBTRACKER_HOST = "jobTrackerHost";
	public static final String JOB_PRIORITY = "JobPriority";
	public static final String JOB_ID = "job_id";
	public static final String MAPERTASKNUMBER = "maper_task_number";
	public static final String MAPER_INPUTDIR = "maper_input_dir";
	public static final String MAPER_OUTPUTDIR = "maper_output_dir";
	public static final String MAPER_SYS_ZK_ADDRESSR = "sys_zk_address";
	public static final String MAPER_SYS_ZK_ROOT_PATH = "sys_zk_root_path";
	private String jobQueue = "default";
	private String jobCmd = "";

	public static void main(String[] args) throws Exception {
		Map<String, String> param = new HashMap<String, String>();
		param.put(HDFS_HOST, "hdfs://hadoop01:9000");
		param.put(JOBTRACKER_HOST, "hadoop01:9001");
		param.put(MAPERTASKNUMBER, "5");
		param.put(JOB_ID, "111");
	}

	/**
	 * 下载、下载后立即处理 1、下载只记录下载的文件相关信息，不记录job运行的日志 2、处理需要记录JOB的日志
	 * 
	 * @param mrconf
	 * 
	 * @param ftpconf
	 * @throws IOException
	 * @throws Exception
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void runDown(Job conf, MRConfiguration mrconf) throws IOException, Exception {
		MRConfiguration ma = new MRConfiguration(conf.getConfiguration());
		List<Class<?>> lstClazz = ma.getOutputFormatClass();
		Class<OutputFormat> outputFormat = this.getOutputFormat(lstClazz);

		FTPConfiguration ftpconf = new FTPConfiguration(conf.getConfiguration());
		conf.getConfiguration().set("fs.hdfs.impl.disable.cache", "true");
		conf.setMapperClass(FTPMaper.class);
		conf.setReducerClass(DBReducer.class);

		// set input
		conf.setMapOutputKeyClass(DBRecord.class);
		conf.setMapOutputValueClass(DBRecord.class);
		conf.setInputFormatClass(FTPInputFormat.class);
		if (ftpconf.getInputIsCombiner()) {
			conf.setCombinerClass(DBGroupReducer.class);
		}

		// set output
		conf.setOutputKeyClass(DBRecord.class);
		conf.setReduceSpeculativeExecution(false);
		conf.setOutputValueClass(NullWritable.class);
		conf.setOutputFormatClass(outputFormat);

		this.jobQueue = conf.getConfiguration().get(JobContext.QUEUE_NAME, "default");
		this.jobCmd = mrconf.getRunJobCmd();

		// -downid 1 下载
		// -downid 1 -downlogid 1001 重新下载，针对失败或者更新文件
		// -downid 1 -downfail 只针对失败的重传
		// -downid 1 -downforce 强制重复下载当前最新的logid

		// 第一步：判断是否处理某次或者所有失败的情况，若需要，则过滤列表
		boolean isForce = ftpconf.getFtpDownCollectForceRepeat();
		long repeatColLogId = ftpconf.getFtpDownCollectRepeatLogId();
		long lastColLogIds = MRLog.getInstance().getJobMRColLastLogId(ftpconf.getFtpDownCollectId());// 该任务的最大日志id
		boolean colFails = ftpconf.getFtpDownFail(); // 下载col_id下所有失败的文件标示符
		if (repeatColLogId <= 0) {
			repeatColLogId = lastColLogIds;
		}

		if (colFails) {
			MRLog.systemOut("下载col_id下所有失败的文件");
			ftpconf.setFtpDownMappedParamNew(this.getRemoveRepeatInputPath(conf, ftpconf,
					ftpconf.getFtpDownMappedParamNew(), ftpconf.getFtpDownCollectId(), -1));
		} else if (isForce || repeatColLogId <= 0) {// 第一次下载(或者强制重复下载)
			MRLog.systemOut("第一次下载或者强制下载");
		} else if (repeatColLogId > 0) {// 指定col_log_id下载失败、更新文件、新增的文件，默认最新的col_log_id
			MRLog.systemOut("下载失败、更新文件、新增的文件，日志ID：" + repeatColLogId);
			ftpconf.setFtpDownMappedParamNew(this.getRemoveRepeatInputPath(conf, ftpconf, repeatColLogId,
					ftpconf.getFtpDownMappedParamNew()));
		}

		List<InputSourceRecordPO> lstInputRecord = null;
		// 解析地址
		try {
			lstInputRecord = (List<InputSourceRecordPO>) SourceUtil.deserializable(ftpconf.getFtpDownMappedParamNew());
		} catch (Exception e) {
			System.err.println(e.getMessage());
			return;
		}

		if (null == lstInputRecord || lstInputRecord.size() <= 0) {
			return;
		}

		// 第二步：初始化或更新并打印下载日志信息
		long newColLogId = -1;
		if (colFails) {
			// 更新输入文件的最后修改时间和文件大小
			this.setInputFileLastModify(conf.getConfiguration(), lstInputRecord);
			SourceUtil.updateColTaskLogInit(lstInputRecord);
		} else if (isForce || repeatColLogId <= 0) {
			newColLogId = SourceUtil.initColTaskLog(lstInputRecord, this.jobQueue, this.jobCmd);
			SourceUtil.printColLogId(newColLogId);

			// 记录错误信息
			SourceUtil.redcordErrorMsg(ftpconf.getFtpRedcordErrorInputMsg(), newColLogId);
			ftpconf.setFtpRedcordErrorInputMsg("");
			SourceUtil.redcordErrorMsg(ftpconf.getFtpRedcordErrorOuputMsg(), newColLogId);
			ftpconf.setFtpRedcordErrorOuputMsg("");

			// 更新输出的colLogId
			SourceUtil.setOutputSourceRecordColLogId(newColLogId, lstInputRecord);
		} else if (repeatColLogId > 0) {
			SourceUtil.updateColTaskLogSomeTimeInit(repeatColLogId);

			// 记录错误信息
			SourceUtil.redcordErrorMsg(ftpconf.getFtpRedcordErrorInputMsg(), repeatColLogId);
			ftpconf.setFtpRedcordErrorInputMsg("");
			SourceUtil.redcordErrorMsg(ftpconf.getFtpRedcordErrorOuputMsg(), repeatColLogId);
			ftpconf.setFtpRedcordErrorOuputMsg("");

			// 更新输出的colLogId
			SourceUtil.setOutputSourceRecordColLogId(repeatColLogId, lstInputRecord);
		}

		// 重新序列化
		ftpconf.setFtpDownMappedParamNew(SourceUtil.serializable(lstInputRecord));

		// 第三步：处理文件来源于本地，本地文件到HDFS
		boolean isAllLocalFilePath = this.downLocalFile(ftpconf);
		if (!isAllLocalFilePath) {
			ftpconf.setCurrentRunFlag(FTPConfiguration.COL_CURRENT_RUN_FLAG_DOWN);
			conf.waitForCompletion(true);
			// JobClient.runJob(conf);
		}

		// 第四步: 记录下载日志信息(更新)
		if (colFails) {
			SourceUtil.updateColTaskLogReult(lstInputRecord);
		} else if (isForce || repeatColLogId <= 0) {
			SourceUtil.updateColTaskLogSomeTimeReult(newColLogId);
		} else if (repeatColLogId > 0) {
			SourceUtil.updateColTaskLogSomeTimeReult(repeatColLogId);
		}
	}

	private void setInputFileLastModify(Configuration conf, List<InputSourceRecordPO> lstInputRecord) {
		for (InputSourceRecordPO inputSourceRecordPO : lstInputRecord) {
			Object objSourceSystem = SourceUtil.initServer(inputSourceRecordPO.getDataSource());
			String protocol = inputSourceRecordPO.getDataSource().getProtocol();
			try {
				SourceUtil.setInputFileLastModify(objSourceSystem, protocol, inputSourceRecordPO);
			} catch (Exception e) {
				MRLog.systemOut("ERROR:" + e.getMessage());
				e.printStackTrace();
			}
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Class<OutputFormat> getOutputFormat(List<Class<?>> lstClazz) {
		if (lstClazz.size() == 1) {
			return (Class<OutputFormat>) lstClazz.get(0);
		}
		for (Class<?> class1 : lstClazz) {
			if (!FTPOutputFormat.class.getCanonicalName().equals(class1.getCanonicalName())) {
				return (Class<OutputFormat>) class1;
			}
		}
		return null;
	}

	/**
	 * 过滤已经下载成功,并且无需强制重复下载的地址
	 * 
	 * @param conf
	 * @param ftpconf
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings("unchecked")
	private String getRemoveRepeatInputPath(Job conf, FTPConfiguration ftpconf, long colLogId, String param)
			throws IOException, ClassNotFoundException {
		List<InputSourceRecordPO> inputAllPath = (List<InputSourceRecordPO>) SourceUtil.deserializable(param);
		List<InputSourceRecordPO> delInputPath = new ArrayList<InputSourceRecordPO>(); // 成功输入
		List<OutputSourceRecordPO> delOutputPath = new ArrayList<OutputSourceRecordPO>();// 成功输出
		boolean isnotmodify = true;
		for (InputSourceRecordPO isrp : inputAllPath) {
			List<OutputSourceRecordPO> list = isrp.getLstOutputSourceRecord();
			delOutputPath.clear();
			boolean isHandleInputSuccess = false;
			int tempLength = 0;
			for (OutputSourceRecordPO osrp : list) {
				// 不需要传输，但需要更新状态
				boolean isNotNeed = true;
				// 判断输出是否成功
				boolean isHandleOutputSuccess = true;
				osrp.setColLogId(colLogId);
				// 得到失败，需要更新
				Object[] mrColDetailFtpLogPoLog = MRLog.getInstance().getJobMRColDetailLog(isrp.getColId(), colLogId,
						isrp.getSourceOnlyFlag(), osrp.getSourceOnlyFlag());
				FTPLogPO ftpLogPO = null;
				MRColDetailLog detailLog = null;
				if (mrColDetailFtpLogPoLog != null) {
					ftpLogPO = (FTPLogPO) mrColDetailFtpLogPoLog[0];
					detailLog = (MRColDetailLog) mrColDetailFtpLogPoLog[1];
				}
				if (null == ftpLogPO) {
					continue;
				}

				tempLength++;
				isNotNeed = (ftpLogPO.getStatus() == FTPLogPO.STATUS_SUCCESS);
				osrp.setTranSuccess(isNotNeed);

				// 是否需要操作输出
				boolean isOutMove = detailLog.getIsMoveOutput() == MRColDetailLog.ISMOVEOUTPUT_YES;
				boolean isOutRename = detailLog.getIsOutputRename() == MRColDetailLog.ISOUTPUTRENAME_YES;
				boolean moveOutState = true;
				boolean renameOutState = true;
				if (isOutMove) {
					moveOutState = detailLog.getMoveOutputStatus() == MRColDetailLog.MOVEOUTPUTSTATUS_SUCCESS;
				}
				if (isOutRename) {
					renameOutState = detailLog.getMoveOutputStatus() == MRColDetailLog.OUTPUTRENAMESTATUS_SUCCESS;
				}
				isHandleOutputSuccess = moveOutState && renameOutState;

				// 是否需要操作输入
				int isDoingInput = detailLog.getIsDoinputfiletype();
				if (isDoingInput == MRColDetailLog.ISDOINPUTFILETYPE_DELETE) {
					isHandleInputSuccess = detailLog.getDeleteInputStatus() == MRColDetailLog.DELETEINPUTSTATUS_SUCCESS;
				} else if (isDoingInput == MRColDetailLog.ISDOINPUTFILETYPE_MOVE) {
					isHandleInputSuccess = detailLog.getMoveInputStatus() == MRColDetailLog.MOVEINPUTSTATUS_SUCCESS;
				} else if (isDoingInput == MRColDetailLog.ISDOINPUTFILETYPE_RENAME) {
					isHandleInputSuccess = detailLog.getRenameInputStatus() == MRColDetailLog.RENAMEINPUTSTATUS_SUCCESS;
				} else if (isDoingInput == MRColDetailLog.ISDOINPUTFILETYPE_MOVE_RENAME) {
					isHandleInputSuccess = (detailLog.getMoveInputStatus() == MRColDetailLog.MOVEINPUTSTATUS_SUCCESS) &&
							(detailLog.getRenameInputStatus() == MRColDetailLog.RENAMEINPUTSTATUS_SUCCESS);
				}

				osrp.setFirst(false);
				String fileDate = ftpLogPO.getInputFileLastModifyTime();
				// ftp传输成功，并且详细日志成功
				isnotmodify = (null != fileDate && fileDate.equals(isrp.getLastModiyTime()));
				if (null != ftpLogPO && isNotNeed && isnotmodify &&
						detailLog.getStatus() == MRColDetailLog.STATUS_SUCCESS) {
					delOutputPath.add(osrp);
				}
				osrp.setHandleOutputSuccess(isHandleOutputSuccess && isnotmodify);
				osrp.setColFileDetailId(ftpLogPO.getColFileDetailId());// 记录上一次的详细日志ID，用于更新数据
			}

			isrp.setFirst(tempLength > 0 ? false : true);
			isrp.setHandleInputSuccess(isHandleInputSuccess && isnotmodify);
			for (OutputSourceRecordPO delopp : delOutputPath) {
				list.remove(delopp);
			}

			if (isrp.getLstOutputSourceRecord().size() <= 0) {
				delInputPath.add(isrp);
				MRLog.systemOut("-------------------移除掉的数据：" + delInputPath.size());
				for (InputSourceRecordPO isTemp : delInputPath) {
					MRLog.systemOut("-------------------移除掉的数据路径：" + isTemp.toString());
				}
			}
		}

		for (InputSourceRecordPO delisrp : delInputPath) {
			inputAllPath.remove(delisrp);
		}
		MRLog.systemOut("------------------------需要启用的MAP数量：" + inputAllPath.size());

		// 测试打印
		MRLog.systemOut("排除无需重复之后的地址列表");
		for (InputSourceRecordPO isrp : inputAllPath) {
			MRLog.systemOut("input:==============" + isrp.toString());
			List<OutputSourceRecordPO> lst = isrp.getLstOutputSourceRecord();
			for (OutputSourceRecordPO outs : lst) {
				MRLog.systemOut("                    output:====" + outs.toString());
			}
		}

		return SourceUtil.serializable(inputAllPath);
	}

	/**
	 * 过滤已经下载成功,并且无需强制重复下载的地址
	 * 
	 * @param conf
	 * @param ftpconf
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private String getRemoveRepeatInputPath(Job conf, FTPConfiguration ftpconf, String param, int colId, long colLogId)
			throws IOException, ClassNotFoundException {
		List<InputSourceRecordPO> inputAllPath = new ArrayList<InputSourceRecordPO>();
		List<InputSourceRecordPO> delInputPath = new ArrayList<InputSourceRecordPO>(); // 成功输入

		// 找出失败的，不满足要求的部分直接过滤掉
		// 获取采集ID下所有最新失败的记录(FTPLogPO, MRColDetailLog)// 还原配置
		List<Object[]> lstFail = MRLog.getInstance().getColIdFailList(colId, colLogId);
		Map<String, InputSourceRecordPO> tmpInputGroup = new HashMap<String, InputSourceRecordPO>();
		for (Object[] mrColDetailFtpLogPoLog : lstFail) {
			if (mrColDetailFtpLogPoLog == null || mrColDetailFtpLogPoLog.length != 2) {
				continue;
			}

			FTPLogPO ftpLogPO = (FTPLogPO) mrColDetailFtpLogPoLog[0];
			MRColDetailLog detailLog = (MRColDetailLog) mrColDetailFtpLogPoLog[1];

			String inputFilemsg = ftpLogPO.getInputFileMsg();
			if (inputFilemsg == null) {
				MRLog.systemOut("输入信息不正确[过滤掉]：" + inputFilemsg);
				continue;
			}
			String inFileMsg[] = inputFilemsg.split(":");
			if (inFileMsg.length != 6) {
				MRLog.systemOut("输入信息不正确[过滤掉]：" + inputFilemsg);
				continue;
			}

			InputSourceRecordPO inRecord = tmpInputGroup.get(inputFilemsg);
			boolean isnotmodify = false;
			if (inRecord == null) {
				inRecord = new InputSourceRecordPO();
				inRecord.setColId((int) detailLog.getColId());

				FTPDataSourcePO ftpDataSourcePO = new FTPDataSourcePO();
				ftpDataSourcePO.setProtocol(inFileMsg[0]);
				ftpDataSourcePO.setIp(inFileMsg[1]);
				ftpDataSourcePO.setPort(inFileMsg[2]);
				ftpDataSourcePO.setUserName(inFileMsg[3]);
				ftpDataSourcePO.setPassword(inFileMsg[4]);

				inRecord.setDataSource(ftpDataSourcePO);
				inRecord.setMovePath(detailLog.getMoveInputPath());
				inRecord.setRootPath(StringUtil.getSlashSuffixPath(detailLog.getInputPath()));
				inRecord.setDotype(detailLog.getIsDoinputfiletype());
				inRecord.setPath(inFileMsg[5]);
				inRecord.setLastModiyTime(ftpLogPO.getInputFileLastModifyTime());
				inRecord.setSize(detailLog.getFileSize());
				inRecord.setInputSourceSystem(null);
				inRecord.setNewName(detailLog.getInputRename());
				inRecord.setLstOutputSourceRecord(new ArrayList<OutputSourceRecordPO>());

				// 状态赋值
				boolean isHandleInputSuccess = false;

				// 是否需要操作输入
				int isDoingInput = detailLog.getIsDoinputfiletype();
				if (isDoingInput == MRColDetailLog.ISDOINPUTFILETYPE_DELETE) {
					isHandleInputSuccess = detailLog.getDeleteInputStatus() == MRColDetailLog.DELETEINPUTSTATUS_SUCCESS;
				} else if (isDoingInput == MRColDetailLog.ISDOINPUTFILETYPE_MOVE) {
					isHandleInputSuccess = detailLog.getMoveInputStatus() == MRColDetailLog.MOVEINPUTSTATUS_SUCCESS;
				} else if (isDoingInput == MRColDetailLog.ISDOINPUTFILETYPE_RENAME) {
					isHandleInputSuccess = detailLog.getRenameInputStatus() == MRColDetailLog.RENAMEINPUTSTATUS_SUCCESS;
				} else if (isDoingInput == MRColDetailLog.ISDOINPUTFILETYPE_MOVE_RENAME) {
					isHandleInputSuccess = (detailLog.getMoveInputStatus() == MRColDetailLog.MOVEINPUTSTATUS_SUCCESS) &&
							(detailLog.getRenameInputStatus() == MRColDetailLog.RENAMEINPUTSTATUS_SUCCESS);
				}

				inRecord.setFirst(false);
				// ftp传输成功，并且详细日志成功
				String fileDate = ftpLogPO.getInputFileLastModifyTime();
				isnotmodify = (null != fileDate && fileDate.equals(inRecord.getLastModiyTime()));
				inRecord.setHandleInputSuccess(isHandleInputSuccess && isnotmodify);

				inputAllPath.add(inRecord);
				tmpInputGroup.put(inputFilemsg, inRecord);
			}

			// 查询输入对应失败的输出列表
			String outputFilemsg = ftpLogPO.getOutputFileMsg();
			if (outputFilemsg == null) {
				MRLog.systemOut("输入信息：" + inputFilemsg + ",输出信息不正确[过滤掉]：" + outputFilemsg);
				continue;
			}
			String outFileMsg[] = outputFilemsg.split(":");
			if (outFileMsg.length != 6) {
				MRLog.systemOut("输入信息：" + inputFilemsg + ",输出信息不正确[过滤掉]：" + outputFilemsg);
				continue;
			}

			List<OutputSourceRecordPO> lstOutRecord = inRecord.getLstOutputSourceRecord();
			OutputSourceRecordPO output = new OutputSourceRecordPO();
			lstOutRecord.add(output);

			// out 基本参数赋值
			output.setColId((int) detailLog.getColId());

			FTPDataSourcePO outputFtpDataSourcePO = new FTPDataSourcePO();
			outputFtpDataSourcePO.setProtocol(outFileMsg[0]);
			outputFtpDataSourcePO.setIp(outFileMsg[1]);
			outputFtpDataSourcePO.setPort(outFileMsg[2]);
			outputFtpDataSourcePO.setUserName(outFileMsg[3]);
			outputFtpDataSourcePO.setPassword(outFileMsg[4]);

			output.setDataSource(outputFtpDataSourcePO);
			output.setMovePath(detailLog.getMoveOutputPath());
			output.setRootPath(StringUtil.getSlashSuffixPath(detailLog.getOutputPath()));
			output.setColLogId(detailLog.getColLogId());
			output.setNewName(detailLog.getOutputRename()); // 设置重命名后的新名称
			output.setPath(outFileMsg[5]);
			output.setCompress(StringUtil.getCompressFlag(outFileMsg[5]));

			// 状态赋值
			output.setTranSuccess(ftpLogPO.getStatus() == FTPLogPO.STATUS_SUCCESS);
			// 是否需要操作输出
			boolean isOutMove = detailLog.getIsMoveOutput() == MRColDetailLog.ISMOVEOUTPUT_YES;
			boolean isOutRename = detailLog.getIsOutputRename() == MRColDetailLog.ISOUTPUTRENAME_YES;
			boolean moveOutState = true;
			boolean renameOutState = true;
			if (isOutMove) {
				moveOutState = detailLog.getMoveOutputStatus() == MRColDetailLog.MOVEOUTPUTSTATUS_SUCCESS;
			}
			if (isOutRename) {
				renameOutState = detailLog.getMoveOutputStatus() == MRColDetailLog.OUTPUTRENAMESTATUS_SUCCESS;
			}

			boolean isHandleOutputSuccess = moveOutState && renameOutState;
			output.setFirst(false);
			output.setHandleOutputSuccess(isHandleOutputSuccess && isnotmodify);
			output.setColFileDetailId(ftpLogPO.getColFileDetailId());// 记录上一次的详细日志ID，用于更新数据
		}

		for (InputSourceRecordPO inputPath : inputAllPath) {
			if (inputPath.getLstOutputSourceRecord().size() <= 0) {
				delInputPath.add(inputPath);
			}
		}

		for (InputSourceRecordPO delisrp : delInputPath) {
			inputAllPath.remove(delisrp);
		}
		MRLog.systemOut("------------------------需要启用的MAP数量：" + inputAllPath.size());

		// 测试打印
		MRLog.systemOut("排除无需重复之后的地址列表");
		for (InputSourceRecordPO isrp : inputAllPath) {
			MRLog.systemOut("input:==============" + isrp.toString());
			List<OutputSourceRecordPO> lst = isrp.getLstOutputSourceRecord();
			for (OutputSourceRecordPO outs : lst) {
				MRLog.systemOut("                    output:====" + outs.toString());
			}
		}

		return SourceUtil.serializable(inputAllPath);
	}

	/**
	 * 下载本地文件到目标，并且移除处理的本地路径列表
	 * 
	 * @param ftpconf
	 * @param lstInputRecord
	 * @return true:只有本地路径
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings("unchecked")
	private boolean downLocalFile(FTPConfiguration ftpconf) throws IOException, ClassNotFoundException,
			InterruptedException {
		List<InputSourceRecordPO> lstInputRecord = (List<InputSourceRecordPO>) SourceUtil.deserializable(ftpconf
				.getFtpDownMappedParamNew());

		// 本地文件上传至LOCALHDFS中
		List<InputSourceRecordPO> tempLocal = new ArrayList<InputSourceRecordPO>();
		Map<InputSourceRecordPO, FileRecordTrans> mapISRPTran = new HashMap<InputSourceRecordPO, FileRecordTrans>();

		// 日志列表
		List<MRColDetailLog> lstMRColDetailLog = new ArrayList<MRColDetailLog>(1000);
		List<FTPLogPO> lstInsertFTPLogPO = new ArrayList<FTPLogPO>(1000);
		List<FTPLogPO> lstUpdateFTPLogPO = new ArrayList<FTPLogPO>(1000);
		for (InputSourceRecordPO isrp : lstInputRecord) {
			// 初始化采集日志.
			if (!Contant.PROTOCOL_LOCAL.equals(isrp.getDataSource().getProtocol())) {
				continue;
			}

			tempLocal.add(isrp);
			List<OutputSourceRecordPO> lstOsrp = isrp.getLstOutputSourceRecord();
			this.outputFromLocal(ftpconf, isrp, lstOsrp, lstMRColDetailLog, lstInsertFTPLogPO, lstUpdateFTPLogPO);
		}

		MRLog.getInstance().addColFileDetailLogBatch(lstMRColDetailLog);
		lstMRColDetailLog.clear();
		MRLog.getInstance().addJobRemoteFileMsgBatch(lstInsertFTPLogPO);
		lstInsertFTPLogPO.clear();
		MRLog.getInstance().updateJobRemoteFileMsgBatch(lstUpdateFTPLogPO);
		lstUpdateFTPLogPO.clear();

		// 传输
		for (InputSourceRecordPO isrp : tempLocal) {
			FileRecordTrans frt = new FileRecordTrans(ftpconf.getConf());
			mapISRPTran.put(isrp, frt);
			frt.inputToOutput(ftpconf.getConf(), isrp, isrp.getLstOutputSourceRecord());
			// 记录文件到文件列表
			frt.addFileToFileList(isrp);
		}

		// 输入，输出文件的处理
		for (InputSourceRecordPO isrp : tempLocal) {
			FileRecordTrans frt = mapISRPTran.get(isrp);
			if (!isrp.isHandleInputSuccess()) {
				frt.handleInputFile(null, isrp);
			}
			List<OutputSourceRecordPO> lstOutput = isrp.getLstOutputSourceRecord();
			for (OutputSourceRecordPO osrp : lstOutput) {
				if (OutputSourceRecordPO.STATUS_SUCCESS == osrp.getStatus() && !osrp.isHandleOutputSuccess()) {
					frt.handleOutputFile(ftpconf.getConf(), osrp);
				}

			}
			// 更新日志
			frt.updateColFileLog(isrp, null);
		}

		for (InputSourceRecordPO isrp : tempLocal) {
			lstInputRecord.remove(isrp);
		}

		// 重新设置输入路径列表
		if (tempLocal.size() > 0) {
			ftpconf.setFtpDownMappedParamNew(SourceUtil.serializable(lstInputRecord));
		}

		return lstInputRecord.size() == 0;
	}

	/**
	 * 本地HDFS到本地
	 * 
	 * @param lstUpdateFTPLogPO
	 * @param lstInsertFTPLogPO
	 * @param lstMRColDetailLog
	 * @throws IOException
	 */
	public void outputLocalFromLocalHDFS(FTPConfiguration ftpconf, InputSourceRecordPO isrp,
			List<OutputSourceRecordPO> lstOsrp, List<MRColDetailLog> lstMRColDetailLog,
			List<FTPLogPO> lstInsertFTPLogPO, List<FTPLogPO> lstUpdateFTPLogPO) throws IOException {
		if (null == isrp || null == lstOsrp || lstOsrp.size() <= 0) {
			return;
		}

		// 初始化详细日志
		SourceUtil.initColFileLog(isrp, lstOsrp, lstMRColDetailLog);
		if (lstMRColDetailLog.size() >= 1000) { // 批量提交
			MRLog.getInstance().addColFileDetailLogBatch(lstMRColDetailLog);
			lstMRColDetailLog.clear();
		}

		// 初始化判断唯一性的日志信息
		SourceUtil.initFTPLogInfo(ftpconf, isrp, lstOsrp, lstInsertFTPLogPO, lstUpdateFTPLogPO);
		if (lstInsertFTPLogPO.size() >= 1000) { // 批量提交
			MRLog.getInstance().addJobRemoteFileMsgBatch(lstInsertFTPLogPO);
			lstInsertFTPLogPO.clear();
		}

		if (lstUpdateFTPLogPO.size() >= 1000) { // 批量提交
			MRLog.getInstance().updateJobRemoteFileMsgBatch(lstUpdateFTPLogPO);
			lstUpdateFTPLogPO.clear();
		}
	}

	/**
	 * 本地到本地HDFS
	 * 
	 * @param lstUpdateFTPLogPO
	 * @param lstInsertFTPLogPO
	 * @param lstMRColDetailLog
	 * @throws IOException
	 */
	public void outputFromLocal(FTPConfiguration ftpconf, InputSourceRecordPO isrp, List<OutputSourceRecordPO> lstOsrp,
			List<MRColDetailLog> lstMRColDetailLog, List<FTPLogPO> lstInsertFTPLogPO, List<FTPLogPO> lstUpdateFTPLogPO)
			throws IOException {
		if (null == isrp || null == lstOsrp || lstOsrp.size() <= 0) {
			return;
		}

		// 初始化详细日志
		SourceUtil.initColFileLog(isrp, lstOsrp, lstMRColDetailLog);
		if (lstMRColDetailLog.size() >= 1000) { // 批量提交
			MRLog.getInstance().addColFileDetailLogBatch(lstMRColDetailLog);
			lstMRColDetailLog.clear();
		}

		// 初始化判断唯一性的日志信息
		SourceUtil.initFTPLogInfo(ftpconf, isrp, lstOsrp, lstInsertFTPLogPO, lstUpdateFTPLogPO);
		if (lstInsertFTPLogPO.size() >= 1000) { // 批量提交
			MRLog.getInstance().addJobRemoteFileMsgBatch(lstInsertFTPLogPO);
			lstInsertFTPLogPO.clear();
		}

		if (lstUpdateFTPLogPO.size() >= 1000) { // 批量提交
			MRLog.getInstance().updateJobRemoteFileMsgBatch(lstUpdateFTPLogPO);
			lstUpdateFTPLogPO.clear();
		}
	}

	/**
	 * 上传文件(从HDFS到OTHER)
	 * 
	 * @param param
	 * @param inputAddress
	 * @param outputAddress
	 * @throws IOException
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public void runUp(Job conf) throws IOException, Exception {
		FTPConfiguration ftpconf = new FTPConfiguration(conf.getConfiguration());

		// 获取系统参数
		String hdfsHost = ftpconf.getUpFSDefault();
		String jobTrackerHost = ftpconf.getUpJobTracker();
		String inputDir = ftpconf.getMapredInputDir();
		String outputDir = ftpconf.getMapredOutputDir();
		int mapTasks = ftpconf.getNumMapTasks();
		JobPriority jobPriority = ftpconf.getTempMapredJobPriority();

		conf.setJobName(ftpconf.getFtpUpCollectName());
		conf.getConfiguration().set(MRConfiguration.FS_DEFAULT_NAME, hdfsHost);
		conf.getConfiguration().set(MRConfiguration.MAPRED_JOB_TRACKER, jobTrackerHost);
		conf.setPriority(jobPriority);
		// .set(JobContext.PRIORITY,
		// org.apache.hadoop.mapreduce.JobPriority.valueOf(jobPriority));

		FileInputFormat.setInputPaths(conf, new Path[] { new Path(hdfsHost + inputDir) });
		FileOutputFormat.setOutputPath(conf, new Path(outputDir));

		conf.getConfiguration().set("fs.hdfs.impl.disable.cache", "true");
		conf.setMapperClass(FTPMaper.class);
		conf.setReducerClass(FTPReducer.class);

		// set input
		conf.setMapOutputKeyClass(FTPRecord.class);
		conf.setMapOutputValueClass(FTPRecord.class);
		conf.setInputFormatClass(FTPInputFormat.class);

		// set output
		conf.setOutputKeyClass(FTPRecord.class);
		conf.setOutputValueClass(NullWritable.class);
		conf.setOutputFormatClass(FTPOutputFormat.class);
		conf.setReduceSpeculativeExecution(false);

		// set task
		conf.getConfiguration().setInt(JobContext.NUM_MAPS, mapTasks);
		conf.setNumReduceTasks(0);
		this.jobQueue = conf.getConfiguration().get(JobContext.QUEUE_NAME, "default");
		this.jobCmd = ftpconf.getRunJobCmd();

		// 第一步：判断是否处理某次或者所有失败的情况，若需要，则过滤列表
		boolean isForce = ftpconf.getFtpUpCollectForceRepeat();
		long repeatColLogId = ftpconf.getFtpUpCollectRepeatLogId();
		long lastColLogIds = MRLog.getInstance().getJobMRColLastLogId(ftpconf.getFtpUpCollectId());// 该任务的最大日志id
		boolean colFails = ftpconf.getFtpUpFail(); // 下载col_id下所有失败的文件标示符
		if (repeatColLogId <= 0) {
			repeatColLogId = lastColLogIds;
		}

		if (colFails) {
			MRLog.systemOut("上传col_id下所有失败的文件");
			ftpconf.setFtpUpMappedParamNew(this.getRemoveRepeatInputPath(conf, ftpconf,
					ftpconf.getFtpUpMappedParamNew(), ftpconf.getFtpUpCollectId(), -1));
		} else if (isForce || repeatColLogId <= 0) {// 第一次下载(或者强制重复下载)
			MRLog.systemOut("上传或者强制上传");
		} else if (repeatColLogId > 0) {// 指定col_log_id下载失败、更新文件、新增的文件，默认最新的col_log_id
			MRLog.systemOut("上传失败、更新文件、新增的文件，日志ID：" + repeatColLogId);
			ftpconf.setFtpUpMappedParamNew(this.getRemoveRepeatInputPath(conf, ftpconf, repeatColLogId,
					ftpconf.getFtpUpMappedParamNew()));
		}

		List<InputSourceRecordPO> lstInputRecord = null;
		// 解析地址
		try {
			lstInputRecord = (List<InputSourceRecordPO>) SourceUtil.deserializable(ftpconf.getFtpUpMappedParamNew());
		} catch (Exception e) {
			System.err.println(e.getMessage());
			return;
		}

		if (null == lstInputRecord || lstInputRecord.size() <= 0) {
			return;
		}

		// 第二步：初始化或更新并打印下载日志信息
		long newColLogId = -1;
		if (colFails) {
			// 更新输入文件的最后修改时间和文件大小
			this.setInputFileLastModify(conf.getConfiguration(), lstInputRecord);
			SourceUtil.updateColTaskLogInit(lstInputRecord);
		} else if (isForce || repeatColLogId <= 0) {
			newColLogId = SourceUtil.initColTaskLog(lstInputRecord, this.jobQueue, this.jobCmd);
			SourceUtil.printColLogId(newColLogId);

			// 记录错误信息
			SourceUtil.redcordErrorMsg(ftpconf.getFtpRedcordErrorInputMsg(), newColLogId);
			ftpconf.setFtpRedcordErrorInputMsg("");
			SourceUtil.redcordErrorMsg(ftpconf.getFtpRedcordErrorOuputMsg(), newColLogId);
			ftpconf.setFtpRedcordErrorOuputMsg("");

			// 更新输出的colLogId
			SourceUtil.setOutputSourceRecordColLogId(newColLogId, lstInputRecord);
		} else if (repeatColLogId > 0) {
			SourceUtil.updateColTaskLogSomeTimeInit(repeatColLogId);

			// 记录错误信息
			SourceUtil.redcordErrorMsg(ftpconf.getFtpRedcordErrorInputMsg(), repeatColLogId);
			ftpconf.setFtpRedcordErrorInputMsg("");
			SourceUtil.redcordErrorMsg(ftpconf.getFtpRedcordErrorOuputMsg(), repeatColLogId);
			ftpconf.setFtpRedcordErrorOuputMsg("");

			// 更新输出的colLogId
			SourceUtil.setOutputSourceRecordColLogId(repeatColLogId, lstInputRecord);
		}

		// 重新序列化
		ftpconf.setFtpUpMappedParamNew(SourceUtil.serializable(lstInputRecord));

		// 第三步：处理文件来源于本地，本地文件到HDFS
		boolean isAllLocalFilePath = this.upHDFSTOLocal(ftpconf);
		if (!isAllLocalFilePath) {
			ftpconf.setCurrentRunFlag(FTPConfiguration.COL_CURRENT_RUN_FLAG_UP);
			conf.waitForCompletion(true);
			// JobClient.runJob(conf);
		}

		// 第四步: 记录下载日志信息(更新)
		if (colFails) {
			SourceUtil.updateColTaskLogReult(lstInputRecord);
		} else if (isForce || repeatColLogId <= 0) {
			SourceUtil.updateColTaskLogSomeTimeReult(newColLogId);
		} else if (repeatColLogId > 0) {
			SourceUtil.updateColTaskLogSomeTimeReult(repeatColLogId);
		}
	}

	/**
	 * 本地HDFS到本地，并且移除处理的本地输出路径列表
	 * 
	 * @param ftpconf
	 * @param lstInputRecord
	 * @return true:只有本地路径
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings("unchecked")
	private boolean upHDFSTOLocal(FTPConfiguration ftpconf) throws IOException, ClassNotFoundException,
			InterruptedException {
		List<InputSourceRecordPO> lstInputRecord = (List<InputSourceRecordPO>) SourceUtil.deserializable(ftpconf
				.getFtpUpMappedParamNew());
		Map<InputSourceRecordPO, FileRecordTrans> mapISRPTran = new HashMap<InputSourceRecordPO, FileRecordTrans>();
		Map<InputSourceRecordPO, List<OutputSourceRecordPO>> mapRemove = new HashMap<InputSourceRecordPO, List<OutputSourceRecordPO>>();

		// 日志列表
		List<MRColDetailLog> lstMRColDetailLog = new ArrayList<MRColDetailLog>(1000);
		List<FTPLogPO> lstInsertFTPLogPO = new ArrayList<FTPLogPO>(1000);
		List<FTPLogPO> lstUpdateFTPLogPO = new ArrayList<FTPLogPO>(1000);

		// 初始化日志
		for (InputSourceRecordPO isrp : lstInputRecord) {
			FileRecordTrans frt = new FileRecordTrans(ftpconf.getConf());
			mapISRPTran.put(isrp, frt);
			List<OutputSourceRecordPO> tempLocal = new ArrayList<OutputSourceRecordPO>();
			List<OutputSourceRecordPO> lstOSRP = isrp.getLstOutputSourceRecord();
			for (OutputSourceRecordPO osrp : lstOSRP) {
				if (Contant.PROTOCOL_LOCAL.equals(osrp.getDataSource().getProtocol())) {
					tempLocal.add(osrp);
				}
			}

			if (tempLocal.size() <= 0) {
				continue;
			}

			mapRemove.put(isrp, tempLocal);
			this.outputLocalFromLocalHDFS(ftpconf, isrp, tempLocal, lstMRColDetailLog, lstInsertFTPLogPO,
					lstUpdateFTPLogPO);
		}

		MRLog.getInstance().addColFileDetailLogBatch(lstMRColDetailLog);
		lstMRColDetailLog.clear();
		MRLog.getInstance().addJobRemoteFileMsgBatch(lstInsertFTPLogPO);
		lstInsertFTPLogPO.clear();
		MRLog.getInstance().updateJobRemoteFileMsgBatch(lstUpdateFTPLogPO);
		lstUpdateFTPLogPO.clear();

		// 传输
		for (InputSourceRecordPO isrp : mapRemove.keySet()) {
			FileRecordTrans frt = mapISRPTran.get(isrp);
			boolean isState = frt.inputToOutput(ftpconf.getConf(), isrp, mapRemove.get(isrp));
			MRLog.systemOut("传输结果：" + (isState ? "成功" : "失败"));
			// 记录文件到文件列表
			frt.addFileToFileList(isrp);
		}

		// 输入，输出文件的处理
		int i = 0;
		for (InputSourceRecordPO isrp : mapRemove.keySet()) {
			FileRecordTrans frts = mapISRPTran.get(isrp);
			List<OutputSourceRecordPO> lstRemoveOutput = mapRemove.get(isrp);
			for (OutputSourceRecordPO osrp : lstRemoveOutput) {
				// 为输出文件进行更新赋值
				if (!osrp.isFirst()) {
					SourceUtil.setOsrp(osrp);
				}
				i++;
				MRLog.systemOut(MRLog.get2Space() + "第<" + i + ">个" + osrp.getFileInfo() + "；传输结果：" +
						(osrp.getStatus() == 0 ? "成功" : "失败") + ", 是否需要处理(根据移动状态，重命名状态，文件更新共同判断)：" +
						(osrp.isHandleOutputSuccess() ? "否" : "是"));
				if (OutputSourceRecordPO.STATUS_SUCCESS == osrp.getStatus() && !osrp.isHandleOutputSuccess()) {
					frts.handleOutputFile(ftpconf.getConf(), osrp);
				}
			}
		}

		MRLog.systemOut(MRLog.get2Space() + "结束处理输出文件[本地]");

		// 移除输出
		List<InputSourceRecordPO> lstRemoveInput = new ArrayList<InputSourceRecordPO>();
		for (InputSourceRecordPO isrp : mapRemove.keySet()) {
			List<OutputSourceRecordPO> lstOutput = isrp.getLstOutputSourceRecord();

			// 添加移除输入队列
			if (lstOutput.size() == mapRemove.get(isrp).size()) {
				lstRemoveInput.add(isrp);
			}
		}

		// 移除输入
		for (InputSourceRecordPO isrp : lstRemoveInput) {
			FileRecordTrans frts = mapISRPTran.get(isrp);
			frts.handleInputFile(ftpconf.getConf(), isrp);
			// 更新日志
			frts.updateColFileLog(isrp, mapRemove.get(isrp));
			lstInputRecord.remove(isrp);
		}

		boolean flag = false;
		if (lstInputRecord.size() <= 0) {
			flag = true;
		} else {
			// 重新设置输入路径列表
			ftpconf.setFtpUpMappedParamNew(SourceUtil.serializable(lstInputRecord));
		}
		return flag;
	}
}
