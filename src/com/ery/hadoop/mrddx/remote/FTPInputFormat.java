package com.ery.hadoop.mrddx.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.ery.hadoop.mrddx.IHandleFormat;
import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.log.MRColDetailLog;
import com.ery.hadoop.mrddx.log.MRColFileErrorLog;
import com.ery.hadoop.mrddx.log.MRJobMapDataLog;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.StringUtil;

public class FTPInputFormat<T extends FTPWritable> extends InputFormat<LongWritable, T> implements Configurable,
		IHandleFormat {
	private Configuration conf;
	private List<InputSourceRecordPO> inputSourcePO;

	@SuppressWarnings("unchecked")
	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		FTPConfiguration ftpconf = new FTPConfiguration(conf);
		try {
			// 下载
			String fileDownftptype = String.valueOf(ftpconf.getFileDownOrDoFTPType());
			if (MRConfiguration.INTERNAL_SYS_FILE_DOWN_OR_DO_FTPTYPE_DOWN.equals(fileDownftptype) ||
					MRConfiguration.INTERNAL_SYS_FILE_DOWN_OR_DO_FTPTYPE_DOWN_AND_DO.equals(fileDownftptype)) {
				this.inputSourcePO = (List<InputSourceRecordPO>) SourceUtil.deserializable(ftpconf
						.getFtpDownMappedParamNew());
			}

			// 上传
			String fileUpftptype = String.valueOf(ftpconf.getFileUPFTPType());
			if (MRConfiguration.INTERNAL_SYS_FILE_UP_FTPTYPE_UP.equals(fileUpftptype)) {
				this.inputSourcePO = (List<InputSourceRecordPO>) SourceUtil.deserializable(ftpconf
						.getFtpUpMappedParamNew());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		MRLog.systemOut("FTPInputFormat.getSplits begin");
		List<SplitPathPO> lstSplitsPath = new ArrayList<SplitPathPO>();
		if (null == this.inputSourcePO) {
			return new ArrayList<InputSplit>();
		}

		Map<SplitPathPO, InputSourceRecordPO> mapSplitISRP = new HashMap<SplitPathPO, InputSourceRecordPO>();
		for (InputSourceRecordPO source : this.inputSourcePO) {
			SplitPathPO splitPathPO = new SplitPathPO();
			splitPathPO.setObj(source);
			lstSplitsPath.add(splitPathPO);
			mapSplitISRP.put(splitPathPO, source);
		}

		if (lstSplitsPath.size() <= 0) {
			return new ArrayList<InputSplit>();
		}

		SplitPathPO paths[] = lstSplitsPath.toArray(new SplitPathPO[0]);
		int numSplits = job.getConfiguration().getInt(MRConfiguration.MAPRED_MAP_TASKS, 1);
		List<InputSplit> splits = new ArrayList<InputSplit>();
		for (int i = 0; i < numSplits; i++) {
			FTPInputSplit split = new FTPInputSplit();
			splits.add(split);
		}
		int fileCount = 0;
		// 按设置任务数拆分
		for (SplitPathPO splitPathPO : paths) {
			FTPInputSplit split = (FTPInputSplit) splits.get(fileCount % numSplits);
			split.addSplitPath(splitPathPO);
			fileCount++;
		}

		// int count = paths.length;// 记录总数
		// int chunks = numSplits;//
		// job.getInt(MRConfiguration.MAPRED_MAP_TASKS, 1);
		// int chunkSize = (count / chunks);// 单位记录数
		//
		// // 行数拆分成n个的组块数，并相应地调整的最后一个块
		// for (int i = 0; i < chunks; i++) {
		// FTPInputSplit split = new FTPInputSplit();
		// List<SplitPathPO> lstTmp = new ArrayList<SplitPathPO>();
		// if ((i + 1) == chunks) {
		// for (int j = i * chunkSize; j < paths.length; j++) {
		// lstTmp.add(paths[j]);
		// }
		// } else {
		// for (int j = i * chunkSize; j < (i * chunkSize) + chunkSize; j++) {
		// lstTmp.add(paths[j]);
		// }
		// }
		//
		// split.setLstSplitPath(lstTmp);
		// splits.add(split);
		// }

		MRLog.systemOut("splits.size:" + splits.size());
		MRLog.systemOut("FTPInputFormat.getSplits end");
		MRConfiguration mrconf = new MRConfiguration(job.getConfiguration());
		FTPConfiguration ftpconf = new FTPConfiguration(job.getConfiguration());

		// 日志列表
		List<MRJobMapDataLog> lstMRJobMapDataLog = new ArrayList<MRJobMapDataLog>(1000);
		List<MRColDetailLog> lstMRColDetailLog = new ArrayList<MRColDetailLog>(1000);
		List<FTPLogPO> lstInsertFTPLogPO = new ArrayList<FTPLogPO>(1000);
		List<FTPLogPO> lstUpdateFTPLogPO = new ArrayList<FTPLogPO>(1000);
		for (SplitPathPO split : mapSplitISRP.keySet()) {
			int fileDownftptype = job.getConfiguration().getInt(MRConfiguration.INTERNAL_SYS_FILE_DOWN_OR_DO_FTPTYPE,
					-1);
			InputSourceRecordPO isrp = mapSplitISRP.get(split);
			if (MRConfiguration.INTERNAL_SYS_FILE_DOWN_OR_DO_FTPTYPE_DOWN_AND_DO
					.equals(String.valueOf(fileDownftptype))) {
				// 初始化处理文件的日志
				MRJobMapDataLog mrJobMapDataLog = this.initMRDataLog(split, isrp, mrconf);
				lstMRJobMapDataLog.add(mrJobMapDataLog);
				if (lstMRJobMapDataLog.size() >= 1000) { // 批量提交
					MRLog.getInstance().addMRJobMapDataLogFTPBatch(lstMRJobMapDataLog);
					lstMRJobMapDataLog.clear();
				}
			}
			// 初始化详细日志
			SourceUtil.initColFileLog(isrp, isrp.getLstOutputSourceRecord(), lstMRColDetailLog);
			if (lstMRColDetailLog.size() >= 1000) { // 批量提交
				MRLog.getInstance().addColFileDetailLogBatch(lstMRColDetailLog);
				lstMRColDetailLog.clear();
			}

			// 初始化判断唯一性的日志信息
			SourceUtil.initFTPLogInfo(ftpconf, isrp, isrp.getLstOutputSourceRecord(), lstInsertFTPLogPO,
					lstUpdateFTPLogPO);
			if (lstInsertFTPLogPO.size() >= 1000) { // 批量提交
				MRLog.getInstance().addJobRemoteFileMsgBatch(lstInsertFTPLogPO);
				lstInsertFTPLogPO.clear();
			}

			if (lstUpdateFTPLogPO.size() >= 1000) { // 批量提交
				MRLog.getInstance().updateJobRemoteFileMsgBatch(lstUpdateFTPLogPO);
				lstUpdateFTPLogPO.clear();
			}
		}

		MRLog.getInstance().addMRJobMapDataLogFTPBatch(lstMRJobMapDataLog);
		MRLog.getInstance().addColFileDetailLogBatch(lstMRColDetailLog);
		MRLog.getInstance().addJobRemoteFileMsgBatch(lstInsertFTPLogPO);
		MRLog.getInstance().updateJobRemoteFileMsgBatch(lstUpdateFTPLogPO);
		MRLog.systemOut("FTPInputFormat.initlog end");
		// 序列化对象
		for (InputSplit ftpInputSplit : splits) {
			for (SplitPathPO splitPathPO : ((FTPInputSplit) ftpInputSplit).getLstSplitPath()) {
				splitPathPO.setRecord(SourceUtil.serializable(splitPathPO.getObj()));
				splitPathPO.setObj(null);
			}
		}
		MRLog.systemOut("FTPInputFormat.seria end");
		return splits;
	}

	/**
	 * 初始化日志信息
	 * 
	 * @param inputSplit
	 * @param job
	 * @param mrconf
	 */
	private MRJobMapDataLog initMRDataLog(SplitPathPO inputSplit, InputSourceRecordPO isrp, MRConfiguration mrconf) {
		List<OutputSourceRecordPO> lst = isrp.getLstOutputSourceRecord();
		long len = isrp.getSize();
		String path = null;
		int dataSourceId = -1;
		for (OutputSourceRecordPO osrp : lst) {
			if (Contant.PROTOCOL_LOCALHDFS.equals(osrp.getDataSource().getProtocol())) {
				path = this.getPath(isrp, osrp);
				dataSourceId = osrp.getDataSource().getId();
				break;
			}
		}

		if (-1 == dataSourceId || null == path) {
			return null;
		}

		String fileId = StringUtil.getFileOnlyKey(path, dataSourceId);
		MRJobMapDataLog dataLog = new MRJobMapDataLog();
		dataLog.setFilePath(path);
		dataLog.setFileSize(len);
		dataLog.setJobid(String.valueOf(mrconf.getSysJobId()));
		dataLog.setStartTime(StringUtil.dateToString(new Date(), StringUtil.DATE_FORMAT_TYPE1));
		dataLog.setStatus(MRJobMapDataLog.STATUS_DOING);
		dataLog.setFileid(fileId);
		dataLog.setJobLogId(mrconf.getJobLogId());
		dataLog.setDataSourceId(dataSourceId);
		MRLog.getInstance().setMRJobMapDataLogId(dataLog);
		inputSplit.setFileLogId(dataLog.getId());// 设置初始日志id
		return dataLog;
	}

	private String getPath(InputSourceRecordPO isrp, OutputSourceRecordPO osrp) {
		String path = isrp.getPath();
		if (null == path || path.trim().length() <= 0) {
			return null;
		}

		if (path.lastIndexOf("/") + 1 > path.length()) {
			return null;
		}

		// 输出目录
		String outputPath = osrp.getPath();
		String reNamefileName = SourceUtil.getRenameResult(osrp);
		String movePath = osrp.getMovePath();
		int temp = outputPath.lastIndexOf("/");
		if (temp == -1 || temp + 1 >= outputPath.length()) {
			return null;
		}

		String fileName = outputPath.substring(temp + 1, outputPath.length());
		String orgPath = outputPath.substring(0, temp);
		fileName = null == reNamefileName ? fileName : reNamefileName;
		fileName = this.getCompressFileName(osrp, fileName);
		if (null != movePath && movePath.trim().length() > 0) {
			return movePath.endsWith("/") ? (movePath + fileName) : (movePath + "/" + fileName);
		} else {
			return orgPath + "/" + fileName;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public RecordReader<LongWritable, T> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new FTPRecordReader<T>((FTPInputSplit) split, (Class<T>) FTPRecord.class, this.getConf());

	}

	/**
	 * A InputSplit that spans a set of rows
	 */
	public static class FTPInputSplit extends InputSplit implements Writable {
		private List<SplitPathPO> lstSplitPath;
		public int currentIndex = 0;

		/**
		 * Convenience Constructor
		 * 
		 * @param start
		 *            the index of the first row to select
		 * @param end
		 *            the index of the last row to select
		 */
		public FTPInputSplit() {
			this.lstSplitPath = new ArrayList<SplitPathPO>();
		}

		@Override
		public String[] getLocations() throws IOException {
			return new String[] {};
		}

		public long getCurFileLogId() {
			return this.lstSplitPath.get(currentIndex).getFileLogId();
		}

		public SplitPathPO getCurSplitPathPO() {
			return this.lstSplitPath.get(currentIndex);
		}

		@Override
		public long getLength() throws IOException {
			return 0;
		}

		public List<SplitPathPO> getLstSplitPath() {
			return lstSplitPath;
		}

		public SplitPathPO[] getArraySplitPath() {
			return this.lstSplitPath == null ? new SplitPathPO[0] : this.lstSplitPath.toArray(new SplitPathPO[0]);
		}

		public void setLstSplitPath(List<SplitPathPO> lstSplitPath) {
			this.lstSplitPath.addAll(lstSplitPath);
		}

		public void addSplitPath(SplitPathPO splitPath) {
			this.lstSplitPath.add(splitPath);
		}

		@Override
		public void readFields(DataInput input) throws IOException {
			int len = input.readInt();
			for (int i = 0; i < len; i++) {
				SplitPathPO spp = new SplitPathPO();
				String record = input.readUTF();
				long fileLogId = input.readLong();
				spp.setRecord(record);
				spp.setFileLogId(fileLogId);
				this.lstSplitPath.add(spp);
			}
		}

		@Override
		public void write(DataOutput output) throws IOException {
			output.writeInt(this.lstSplitPath.size());
			for (SplitPathPO spp : this.lstSplitPath) {
				output.writeUTF(spp.getRecord());
				output.writeLong(spp.getFileLogId());
			}
		}
	}

	@Override
	public void handle(Job conf) throws Exception {
		FTPConfiguration ftpconf = new FTPConfiguration(conf.getConfiguration());
		String ftpInputParam = null;

		// 下载参数
		String fileDownftptype = String.valueOf(ftpconf.getFileDownOrDoFTPType());
		if (MRConfiguration.INTERNAL_SYS_FILE_DOWN_OR_DO_FTPTYPE_DOWN.equals(fileDownftptype) ||
				MRConfiguration.INTERNAL_SYS_FILE_DOWN_OR_DO_FTPTYPE_DOWN_AND_DO.equals(fileDownftptype)) {
			ftpInputParam = ftpconf.getFtpDownMappedParam();
			this.validate(conf, ftpconf, ftpInputParam, FTPConfiguration.CONSTANT_COL_TYPE_DOWN);
		}

		// 上传参数
		String fileUpftptype = String.valueOf(ftpconf.getFileUPFTPType());
		if (MRConfiguration.INTERNAL_SYS_FILE_UP_FTPTYPE_UP.equals(fileUpftptype)) {
			ftpInputParam = ftpconf.getFtpUpMappedParam();
			this.validate(conf, ftpconf, ftpInputParam, FTPConfiguration.CONSTANT_COL_TYPE_UP);
		}
	}

	/**
	 * 验证FTP服务参数
	 * 
	 * @param conf
	 * @param ftpconf
	 * @param colType2
	 * @param fileftptype
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	private void validate(Job conf, FTPConfiguration ftpconf, String ftpInputParam, int colType) throws Exception {
		MRLog.systemOut("开始 验证FTP输入参数");

		// 验证输入
		if (null == ftpInputParam || ftpInputParam.trim().length() <= 0) {
			throw new Exception("FTP input address is null");
		}

		Object objt = SourceUtil.deserializable(ftpInputParam);
		if (!(objt instanceof List<?>)) {
			throw new Exception("ftp input param decode inner error");
		}

		if (((List<?>) objt).size() <= 0) {
			throw new Exception("ftp input param'size is 0, please check ftp param");
		}

		// 打印地址解析结果
		for (Object s : (List<?>) objt) {
			if (s instanceof DBSourceRecordPO) {
				MRLog.systemOut("               info:" + s.toString());
			} else {
				throw new Exception("ftp input param decode inner type error");
			}
		}
		List<DBSourceRecordPO> dbValues = (List<DBSourceRecordPO>) objt;

		// 标示输入是否只有一个(true:一个)
		boolean isOnlyInput = dbValues.size() == 1 ? true : false;

		// 验证协议、协议和根地址组合是否唯一
		SourceUtil.validateProtocol(dbValues);

		// 1、合并地址:一个输入对应多个输出
		Map<DBSourceRecordPO, List<DBSourceRecordPO>> temp = new HashMap<DBSourceRecordPO, List<DBSourceRecordPO>>();
		Map<String, DBSourceRecordPO> tem = new HashMap<String, DBSourceRecordPO>();
		for (DBSourceRecordPO dbsrp : dbValues) {
			String inputUniqueKey = dbsrp.getInputUniqueKey();
			DBSourceRecordPO dbs = tem.get(inputUniqueKey);
			List<DBSourceRecordPO> lst = null;
			if (dbs == null) {
				tem.put(inputUniqueKey, dbsrp);
				lst = new ArrayList<DBSourceRecordPO>();
				temp.put(dbsrp, lst);
				dbs = dbsrp;
			}

			lst = temp.get(dbs);
			if (lst != null) {
				lst.add(dbsrp);
			}
		}

		// 测试打印
		for (DBSourceRecordPO in : temp.keySet()) {
			MRLog.systemOut("input:==============" + in.toInputString());
			List<DBSourceRecordPO> lst = temp.get(in);
			for (DBSourceRecordPO dbSourceRecordPO : lst) {
				MRLog.systemOut("                    output:====" + dbSourceRecordPO.toOuputString());
			}
		}

		// 2、验证一个输入对应的输出中不能存在相同的
		for (DBSourceRecordPO dbsrpo : temp.keySet()) {
			List<DBSourceRecordPO> lst = temp.get(dbsrpo);
			Map<String, DBSourceRecordPO> mapTmp = new HashMap<String, DBSourceRecordPO>();
			for (DBSourceRecordPO dbSourceRecordPO : lst) {
				String outputUnique = dbSourceRecordPO.getOutputUniqueKey();
				if (null != mapTmp.get(outputUnique)) {
					throw new Exception("FTPPath is error, outpath is not unique; PATH:" + dbSourceRecordPO.toString());
				}

				mapTmp.put(outputUnique, dbSourceRecordPO);
			}
		}

		// 3、初始化输入和输出资源
		Map<String, Object> mapSourceSystem = new HashMap<String, Object>();
		for (DBSourceRecordPO dbsrpo : temp.keySet()) {
			// 获取输入资源
			FTPDataSourcePO inSource = dbsrpo.getInputDatasource();
			Object inSourceSystem = mapSourceSystem.get(inSource.getSourceSystemOnlyFlag());
			if (null != inSourceSystem) {
				dbsrpo.setInputSourceSystem(inSourceSystem);
			} else {
				if (!mapSourceSystem.containsKey(inSource.getSourceSystemOnlyFlag())) {
					inSourceSystem = SourceUtil.initServer(inSource);
				}

				if (!Contant.PROTOCOL_LOCAL.equals(inSource.getProtocol()) && null == inSourceSystem) {
					dbsrpo.setInputStatus(0);
				}
				mapSourceSystem.put(inSource.getSourceSystemOnlyFlag(), inSourceSystem);
				dbsrpo.setInputSourceSystem(inSourceSystem);
			}

			for (DBSourceRecordPO dbSourceRecordPO : temp.get(dbsrpo)) {
				// 获取出资源
				FTPDataSourcePO outSource = dbSourceRecordPO.getOutputDatasource();
				Object outSourceSystem = mapSourceSystem.get(outSource.getSourceSystemOnlyFlag());
				if (null != outSourceSystem) {
					dbSourceRecordPO.setOutputSourceSystem(outSourceSystem);
				} else {
					if (!mapSourceSystem.containsKey(outSource.getSourceSystemOnlyFlag())) {
						outSourceSystem = SourceUtil.initServer(outSource);
					}

					if (!Contant.PROTOCOL_LOCAL.equals(outSource.getProtocol()) && null == outSourceSystem) {
						dbSourceRecordPO.setOutputStatus(0);
					}
					mapSourceSystem.put(outSource.getSourceSystemOnlyFlag(), outSourceSystem);
					dbSourceRecordPO.setOutputSourceSystem(outSourceSystem);
				}
			}
		}
		this.exception(dbValues, "FTPPath is init server error.");

		// 测试打印
		// MRLog.systemOut("打印输入和输出的对应关系");
		// for (DBSourceRecordPO in : temp.keySet()) {
		// MRLog.systemOut("input:==============" + in.toInputString());
		// List<DBSourceRecordPO> lst = temp.get(in);
		// for (DBSourceRecordPO dbSourceRecordPO : lst) {
		// MRLog.systemOut("                    output:====" +
		// dbSourceRecordPO.toOuputString());
		// }
		// }

		// 验证输出目录是否正确和获取数据源下的输入路径列表
		Map<String, Set<String>> mapSourceInputpath = new HashMap<String, Set<String>>();
		for (DBSourceRecordPO dbsrpo : temp.keySet()) {
			// 获取数据源下的输入路径列表
			String sourceKey = dbsrpo.getInputDatasource().getSourceSystemOnlyFlag();
			Set<String> setPath = mapSourceInputpath.get(sourceKey);
			if (null == setPath) {
				setPath = new HashSet<String>();
				mapSourceInputpath.put(sourceKey, setPath);
				setPath.add(dbsrpo.getInputPath());
			} else {
				setPath.add(dbsrpo.getInputPath());
			}

			// 验证输出目录是否正确
			for (DBSourceRecordPO dbSourceRecordPO : temp.get(dbsrpo)) {
				SourceUtil.validateOutputRootPath(conf.getConfiguration(), dbSourceRecordPO);
			}
		}
		this.exception(dbValues, "FTPPath is input path error:");

		boolean colFails = ftpconf.getFtpDownFail(); // 下载col_id下所有失败的文件标示符
		if (colFails) {
			MRLog.systemOut("下载所有失败的，无需初始化输入路径的文件列表");
			return;
		}

		// 4、获取所有的输入文件路径
		for (DBSourceRecordPO dbsrpo : temp.keySet()) {
			List<FileAttributePO> lst = SourceUtil.getEffectiveInputAllFilePath(dbsrpo, mapSourceInputpath);
			dbsrpo.setInputListFile(lst);
		}

		// 测试打印
		// MRLog.systemOut("打印输入的地址列表");
		// for (DBSourceRecordPO dbsrpo : temp.keySet()) {
		// MRLog.systemOut("input:==============" + dbsrpo.getInputUniqueKey());
		// List<FileAttributePO> list = dbsrpo.getInputListFile();
		// for (FileAttributePO fileAttributePO : list) {
		// MRLog.systemOut("                    filePath:====" +
		// fileAttributePO.toString());
		// }
		// }

		List<InputSourceRecordPO> lstInputAllRecord = new ArrayList<InputSourceRecordPO>();
		// 5、解析一个输入具体的文件对应的多个输出
		for (DBSourceRecordPO dbsrpo : temp.keySet()) {
			List<FileAttributePO> lstFile = dbsrpo.getInputListFile();
			for (FileAttributePO afb : lstFile) {
				InputSourceRecordPO inRecord = new InputSourceRecordPO();
				inRecord.setColId(dbsrpo.getColId());
				inRecord.setDataSource(dbsrpo.getInputDatasource());
				inRecord.setMovePath(dbsrpo.getInputMovePath());
				inRecord.setRootPath(StringUtil.getSlashSuffixPath(dbsrpo.getInputPath()));
				inRecord.setRenameRule(dbsrpo.getInputRenameRule());
				inRecord.setRename(dbsrpo.getInputRename());
				inRecord.setDotype(dbsrpo.getInputDotype());
				inRecord.setFileRule(dbsrpo.getInputFileRule());
				inRecord.setPath(afb.getPath());
				inRecord.setLastModiyTime(afb.getLastModifyTime());
				inRecord.setSize(afb.getSize());
				inRecord.setInputSourceSystem(null);
				inRecord.setNewName(this.getInputFileNewName(inRecord.getPath(), SourceUtil.getRenameResult(inRecord)));

				// 设置对应的输出列表
				List<OutputSourceRecordPO> lstOutRecord = inRecord.getLstOutputSourceRecord();
				List<DBSourceRecordPO> lstOutTmp = temp.get(dbsrpo);
				for (DBSourceRecordPO outsr : lstOutTmp) {
					OutputSourceRecordPO output = new OutputSourceRecordPO();
					output.setColId(outsr.getColId());
					output.setDataSource(outsr.getOutputDatasource());
					output.setMovePath(outsr.getOutputMovePath());
					output.setOutputSourceSystem(null);
					output.setRenameRule(outsr.getOutputRenameRule());
					output.setRename(outsr.getOutputRename());
					output.setRootPath(StringUtil.getSlashSuffixPath(outsr.getOutputPath()));
					output.setCompress(outsr.getCompress());
					lstOutRecord.add(output);

					this.setOutputSourceRecordPath(inRecord, output);// 设置输出文件路径
					output.setNewName(this.getCompressFileName(output, SourceUtil.getRenameResult(output))); // 设置重命名后的新名称
					output.setPath(this.getCompressFileName(output, output.getPath()));
				}

				lstInputAllRecord.add(inRecord);
			}
		}

		// 测试打印
		// MRLog.systemOut("打印待传输之前的地址列表");
		// for (InputSourceRecordPO isrp : lstInputAllRecord) {
		// MRLog.systemOut("input:==============" + isrp.toString());
		// List<OutputSourceRecordPO> lst = isrp.getLstOutputSourceRecord();
		// for (OutputSourceRecordPO outs : lst) {
		// MRLog.systemOut("                    output:====" + outs.toString());
		// }
		// }

		// 6、处理子目录与父目录
		// 处理本地路径(合并目录与子目录) 根目录:{文件地址，过滤表达式}
		List<InputSourceRecordPO> effectInputAllFiles = null;
		if (isOnlyInput) {
			effectInputAllFiles = lstInputAllRecord;
		} else {
			effectInputAllFiles = SourceUtil.getEffectiveInputAllFilePaths(lstInputAllRecord);
		}

		// 测试打印
		if (ftpconf.getLogConfColIsPrintConsole()) {
			MRLog.systemOut("打印待传输的地址列表");
			for (InputSourceRecordPO isrp : effectInputAllFiles) {
				MRLog.systemOut("input:==============" + isrp.toString());
				List<OutputSourceRecordPO> lst = isrp.getLstOutputSourceRecord();
				for (OutputSourceRecordPO outs : lst) {
					MRLog.systemOut("                    output:====" + outs.toString());
				}
			}
		}

		// 6、 设置需要传输的文件列表, 序列化对象，在split时，再按将一个文件(一个文件的输入输出)序列化传入map
		if (FTPConfiguration.CONSTANT_COL_TYPE_DOWN == colType) {// 下载
			ftpconf.setFtpDownMappedParamNew(SourceUtil.serializable(effectInputAllFiles));
		} else if (FTPConfiguration.CONSTANT_COL_TYPE_UP == colType) {// 上传
			ftpconf.setFtpUpMappedParamNew(SourceUtil.serializable(effectInputAllFiles));
		}

		MRLog.systemOut("结束 验证FTP输入参数");

		// 记录错误输入日志信息
		ftpconf.setFtpRedcordErrorInputMsg(this.getRedcordErrorInputMsg(dbValues));
		ftpconf.setFtpRedcordErrorOuputMsg(this.getRedcordErrorOutputMsg(dbValues));

		// 关闭资源
		SourceUtil.closeInputSourceSystem(dbValues);
	}

	/**
	 * 设置输出文件路径
	 * 
	 * @param inRecord
	 * @param output
	 */
	private void setOutputSourceRecordPath(InputSourceRecordPO inRecord, OutputSourceRecordPO output) {
		String inputFilePath = inRecord.getPath();
		String inputFileRootPath = inRecord.getRootPath();
		String fileName = "";

		String outputRootPath = output.getRootPath();
		// 获取文件名称(以输入的根路径为标准)
		if (!inputFilePath.equals(inputFileRootPath)) {
			fileName = inputFilePath.substring(inputFileRootPath.length(), inputFilePath.length());
		} else {
			fileName = inputFilePath.substring(inputFilePath.lastIndexOf("/") + 1);
		}

		output.setPath(outputRootPath + fileName);
	}

	/**
	 * 获取输出的压缩文件名称
	 * 
	 * @param output
	 * @param fileName
	 * @return
	 */
	private String getCompressFileName(OutputSourceRecordPO output, String fileName) {
		if (null == fileName) {
			return null;
		}
		switch (output.getCompress()) {
		case 1:
			if (!fileName.toLowerCase().endsWith(".gz")) {
				fileName += ".gz";
			}
			break;
		default:
			break;
		}
		return fileName;
	}

	/**
	 * 获取输入的压缩文件重命名名称
	 * 
	 * @param path
	 * @param renameResult
	 * @return
	 */
	private String getInputFileNewName(String path, String renameResult) {
		if (path != null && null != renameResult) {
			if (path.toLowerCase().endsWith(".tar.gz")) {
				renameResult += ".tar.gz";
			} else if (path.toLowerCase().endsWith(".gz")) {
				renameResult += ".gz";
			} else {
				return renameResult;
			}
		}

		return renameResult;
	}

	/**
	 * 获取记录错误信息
	 * 
	 * @param dbValues
	 * @throws IOException
	 */
	private String getRedcordErrorInputMsg(List<DBSourceRecordPO> dbValues) throws IOException {
		List<MRColFileErrorLog> lst = new ArrayList<MRColFileErrorLog>();
		for (DBSourceRecordPO dbsr : dbValues) {
			if (!dbsr.isInputRight()) {
				MRColFileErrorLog mr = new MRColFileErrorLog();
				mr.setCreatTime(StringUtil.dateToString(new Date(), StringUtil.DATE_FORMAT_TYPE1));
				mr.setProtocol(dbsr.getInputDatasource().getProtocol());
				mr.setIp(dbsr.getInputDatasource().getIp());
				mr.setPort(StringUtil.stringToInt(dbsr.getInputDatasource().getPort(), -1));
				mr.setUsername(dbsr.getInputDatasource().getUserName());
				mr.setRootpath(dbsr.getInputPath());
				mr.setType(MRColFileErrorLog.TYPE_DOWN);
				mr.setMsg(String.valueOf(dbsr.getInputStatus()));
				lst.add(mr);
			}
		}
		return SourceUtil.serializable(lst);
	}

	/**
	 * 获取记录错误信息
	 * 
	 * @param dbValues
	 * @throws IOException
	 */
	private String getRedcordErrorOutputMsg(List<DBSourceRecordPO> dbValues) throws IOException {
		List<MRColFileErrorLog> lst = new ArrayList<MRColFileErrorLog>();
		for (DBSourceRecordPO dbsr : dbValues) {
			if (!dbsr.isOutputRight()) {
				MRColFileErrorLog mr = new MRColFileErrorLog();
				mr.setCreatTime(StringUtil.dateToString(new Date(), StringUtil.DATE_FORMAT_TYPE1));
				mr.setProtocol(dbsr.getOutputDatasource().getProtocol());
				mr.setIp(dbsr.getOutputDatasource().getIp());
				mr.setPort(StringUtil.stringToInt(dbsr.getOutputDatasource().getPort(), -1));
				mr.setUsername(dbsr.getOutputDatasource().getUserName());
				mr.setRootpath(dbsr.getOutputPath());
				mr.setType(MRColFileErrorLog.TYPE_UP);
				mr.setMsg(String.valueOf(dbsr.getOutputStatus()));
				lst.add(mr);
			}
		}
		return SourceUtil.serializable(lst);
	}

	/**
	 * 判断是否抛出异常
	 * 
	 * @param dbValues
	 * @param msg
	 * @throws Exception
	 */
	private void exception(List<DBSourceRecordPO> dbValues, String msg) throws Exception {
		if (true) {
			for (DBSourceRecordPO dbSourceRecordPO : dbValues) {
				if (!dbSourceRecordPO.isInputRight()) {
					throw new Exception(msg);
				}
			}
		}
	}
}