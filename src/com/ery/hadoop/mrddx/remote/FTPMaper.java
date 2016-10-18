package com.ery.hadoop.mrddx.remote;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.util.StringUtils;

import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.IDBMonitor;
import com.ery.hadoop.mrddx.IDBSenior;
import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.file.FileConfiguration;
import com.ery.hadoop.mrddx.file.FileSplit;
import com.ery.hadoop.mrddx.file.LineRecordReader;
import com.ery.hadoop.mrddx.file.RCFileParse;
import com.ery.hadoop.mrddx.file.RCFileRecordReader;
import com.ery.hadoop.mrddx.file.SequenceFileRecordReader;
import com.ery.hadoop.mrddx.file.TextParse;
import com.ery.hadoop.mrddx.log.MRColDetailLog;
import com.ery.hadoop.mrddx.log.MRJobMapDataLog;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.remote.FTPInputFormat.FTPInputSplit;
import com.ery.hadoop.mrddx.remote.plugin.IRemotePlugin;
import com.ery.hadoop.mrddx.senior.IMRSeniorApply;
import com.ery.hadoop.mrddx.senior.MRSeniorOperationFilter;
import com.ery.hadoop.mrddx.senior.MRSeniorUtil;
import com.ery.hadoop.mrddx.util.HDFSUtils;
import com.ery.hadoop.mrddx.util.StringUtil;
import com.ery.hadoop.mrddx.zk.IMonitorZKNode;
import com.ery.hadoop.mrddx.zk.MRWatcher;
import com.ery.hadoop.mrddx.zk.MonitorZKNode;

public class FTPMaper extends Mapper<LongWritable, FTPRecord, DBRecord, DBRecord> implements IDBMonitor, IDBSenior {
	public static final Log LOG = LogFactory.getLog(FTPMaper.class);
	public static final MRLog MRLOG = MRLog.getInstance();

	protected FTPConfiguration ftpconf;

	// 监听节点对象
	protected IMonitorZKNode monitorZKNode;

	// 是否需要监控
	protected boolean isNeedZKMonitor;

	// jobID
	protected long jobId;

	// 系统运行的jobID
	protected String sysJobId;

	// 任务ID
	protected String taskId;

	// 0:文本文件、1:sequence文件、2:rcfile文件
	protected int filetype;

	// 是否解析文件(只有当下载的文件到HDFS时，才有效)
	protected boolean isParse;

	// job
	protected Configuration job;

	// 文本解析器
	protected TextParse textParse;

	// RC文本解析器
	protected RCFileParse rcParse;

	/**
	 * 以下为日志参数
	 */
	public int debug = -1; // 日志级别 1：打印job配置参数,
	// 2:打印job解析后的参数(包含1)，3：打印map和redue的输入输出记录

	public int debugRowNum = -1; // 每个map输入条数限制(设置-debug参数时，才生效, 默认值：1000)

	// map执行是否成功, 取值范围[1-2]:1-成功,2-失败
	protected int runflag = 1;

	// job的日志id号
	protected long jobLogId;

	// 当前map是否实际运行过(true-运行过,false-未运行过)
	protected boolean isRun;

	// 输出的记录数
	protected long outCount;

	// 实时记录输入数据量
	protected long perInputNumber;

	// 实时记录输入数据量
	protected long perOutputNumber;

	// 记录数据的日志id，不是文件ID
	protected long fileDataLogId;

	// 过滤数据存放的根路径
	protected String filterFileRootPath;

	// 针对单个文件而言的输出的记录数
	protected long perFileOutCount;

	/**
	 * 与解析相关的参数
	 */
	// 读取到的记录数
	protected long totalRecordCount;

	// 是否map直接输出结果（true：直接输出）
	protected boolean inputMapEnd;

	// 输入字段名称
	protected String[] srcFieldNames;

	// 拆分列的分隔符
	protected String fieldSplitChars;

	// 拆分行记录分隔符
	protected String rowSplitChars;

	// 目标字段名称
	protected String[] destFieldNames;

	// 直接输出的输出字段默认值
	Map<String, String> outColumnDefaultValue;
	// 源字段名称
	protected String[] destRelSrcFieldNames;

	// 字段方法名称
	protected String[] groupFieldMethod;

	// 高级应用对象
	protected IMRSeniorApply mrSeniorApply[];

	// 采集插件对象
	protected IRemotePlugin remotePlugin;
	String parseLocalFilePath;

	FTPInputSplit fileSplit;

	// 临时记录本地输出（mr中除了更新日志外，其他都不处理）
	protected List<OutputSourceRecordPO> lstLocalOutput = new ArrayList<OutputSourceRecordPO>();

	protected void map(LongWritable key, FTPRecord value, Context context) throws IOException, InterruptedException {
		// 监听任务节点
		if (this.isNeedZKMonitor && null != this.monitorZKNode) {
			this.monitorZKNode.monitorTask();
		}

		// 实际运行标识
		if (!this.isRun) {// 新增日志
			this.isRun = true;
		}
		this.fileDataLogId = fileSplit.getCurFileLogId();

		String record = (String) value.getData(FTPRecordReader.RECORD);
		InputSourceRecordPO isrp = null;
		try {
			isrp = (InputSourceRecordPO) SourceUtil.deserializable(record);
		} catch (IOException e2) {
			e2.printStackTrace();
			return;
		}
		MRLog.systemOut("第一步：配置信息");
		MRLog.systemOut(MRLog.get2Space() + isrp.getMetaInfo());
		List<OutputSourceRecordPO> lstOut = isrp.getLstOutputSourceRecord();
		parseLocalFilePath = getLocalHDFSFile(lstOut);

		MRLog.systemOut(MRLog.get2Space() + "输出列表");
		for (int i = 0; i < lstOut.size(); i++) {
			OutputSourceRecordPO osrp = lstOut.get(i);
			if (Contant.PROTOCOL_LOCAL.equals(osrp.getDataSource().getProtocol())) {
				this.lstLocalOutput.add(osrp);
				continue;
			}
			MRLog.systemOut(MRLog.get4Space() + "第<" + (i + 1) + ">个" + osrp.getMetaInfo());
		}
		lstOut.removeAll(lstLocalOutput);
		// for (OutputSourceRecordPO outputSourceRecordPO : this.lstLocalOutput)
		// {
		// lstOut.remove(outputSourceRecordPO);
		// }
		MRLog.systemOut(MRLog.get4Space() + "临时移除本地输出个数：" + this.lstLocalOutput.size());

		FileRecordTrans frt = new FileRecordTrans((context));
		frt.map = this;
		frt.setConf(ftpconf.getConf());
		frt.setRemotePlugin(this.remotePlugin);
		try {
			MRLog.systemOut("第二步：开始传输数据");
			boolean isState = frt.inputToOutput(this.job, isrp, lstOut);
			MRLog.systemOut("传输结果：" + (isState ? "成功" : "失败"));

			// 输入引起的错误对输出的设置
			if (!isState) {
				for (OutputSourceRecordPO tempo : lstOut) {
					tempo.setStatus(OutputSourceRecordPO.STATUS_GET_SYSTEMSOURCE_FAIL);
				}
			}

			// 数据传输成功后的输入文件的处理
			boolean flag = true;
			long detailLogId = 0;
			MRColDetailLog mrColDetailLog = null;

			// 输出文件的处理（先处理输出文件）
			MRLog.systemOut("第三步：开始处理输出文件");
			int i = 0;
			for (OutputSourceRecordPO osrp : lstOut) {
				// 为输出文件进行更新赋值
				if (!osrp.isFirst()) {
					SourceUtil.setOsrp(osrp);
				}
				i++;
				MRLog.systemOut(MRLog.get2Space() + "第<" + i + ">个" + osrp.getFileInfo() + "；传输结果：" +
						(osrp.getStatus() == 0 ? "成功" : "失败") + ", 是否需要处理(根据移动状态，重命名状态，文件更新共同判断)：" +
						(osrp.isHandleOutputSuccess() ? "否" : "是"));
				if (OutputSourceRecordPO.STATUS_SUCCESS == osrp.getStatus() && !osrp.isHandleOutputSuccess()) {
					frt.handleOutputFile(job, osrp);
				}
			}
			MRLog.systemOut(MRLog.get2Space() + "结束处理输出文件");
			// 关闭输出资源
			MRLog.systemOut(MRLog.get2Space() + "开始关闭输出资源");
			frt.closeOutputSource(lstOut);
			MRLog.systemOut(MRLog.get2Space() + "结束关闭输出资源");

			for (OutputSourceRecordPO osrp : lstOut) {
				detailLogId = osrp.getColFileDetailId();
				mrColDetailLog = MRLOG.getDetailLog(detailLogId);

				MRLog.systemOut("osrp.getStatus=" + osrp.getStatus() + "  lstOut.size=" + lstOut.size());
				MRLog.systemOut("mrColDetailLog.getStatus=" + mrColDetailLog.getStatus());
				boolean tem = (osrp.getStatus() == OutputSourceRecordPO.STATUS_SUCCESS) ||
						mrColDetailLog.getStatus() == MRColDetailLog.STATUS_SUCCESS;
				osrp.setStatus(tem ? OutputSourceRecordPO.STATUS_SUCCESS : osrp.getStatus());
				if (OutputSourceRecordPO.STATUS_SUCCESS != osrp.getStatus() ||
						(osrp.isNeedMove() && !osrp.isMovePathStatus()) ||
						(osrp.isNeedMoveRename() && !osrp.isRenameRuleStatus())) {
					MRLog.systemOut("osrp.getStatus=" + osrp.getStatus());
					MRLog.systemOut(" osrp.isNeedMove=" + osrp.isNeedMove() + "osrp.isRenameRuleStatus=" +
							osrp.isRenameRuleStatus());
					MRLog.systemOut(" osrp.isNeedMoveRename=" + osrp.isNeedMoveRename() + "osrp.isMovePathStatus=" +
							osrp.isMovePathStatus());
					flag = false;
					break;
				}
			}

			// 为更新输入进行初始化
			if (!isrp.isFirst()) {
				SourceUtil.setIsrp(isrp, mrColDetailLog);
			}

			MRLog.systemOut("第四步：开始处理输入文件");
			MRLog.systemOut(MRLog.get2Space() + "传输和输出处理是否所有都成功:" + (flag ? "是" : "否") +
					", 是否需要处理(根据移动状态，重命名状态，文件更新共同判断):" + (isrp.isHandleInputSuccess() ? "否" : "是"));
			if (flag && !isrp.isHandleInputSuccess()) {
				frt.handleInputFile(job, isrp);
			}
			MRLog.systemOut(MRLog.get2Space() + "结束处理输入文件");
			// 关闭输入资源
			MRLog.systemOut(MRLog.get2Space() + "开始关闭输入资源");
			frt.closeInputSource(isrp);
			MRLog.systemOut(MRLog.get2Space() + "结束关闭输入资源");

			MRLog.systemOut("第五步：打印最终输出文件列表");
			for (OutputSourceRecordPO tempo : lstOut) {
				MRLog.systemOut(tempo.getFileResultInfo());
			}

			// 记录文件到文件列表
			frt.addFileToFileList(isrp);

			// 更新日志详细信息
			for (OutputSourceRecordPO outputSourceRecordPO : this.lstLocalOutput) {
				// 未拷贝到本地主机
				lstOut.add(outputSourceRecordPO);
			}
			MRLog.systemOut(MRLog.get4Space() + "重新加入临时本地个数：" + this.lstLocalOutput.size());

			MRLog.systemOut("第六步：更新日志信息.");
			frt.updateColFileLog(isrp, null);
			MRLog.systemOut("传输文件结束.");
			// 判断是否需要解析
			MRLog.systemOut("是否解析数据：" + (this.isParse ? "是" : "否"));
			if (!isParse) {
				return;
			}
			// 解析文件
			MRLog.systemOut("isrp===>        isrp=" + isrp);
			if (null == isrp || lstOut.size() <= 0) {
				return;
			}
			MRLog.systemOut("isrp===>        isrp.size=" + lstOut.size());
			// add to data run log
			if (!frt.isParse) {// 是否在下载过程中已经解析了
				this.isRun = false;
				this.parseFile(context, lstOut, frt.lineCount);
			} else {// 只补日志
				if (parseLocalFilePath == null)
					return;
				MRLog.systemOut("解析已在下载过程中执行，最后只更新日志信息.");
				Path path = new Path(StringUtils.unEscapeString(parseLocalFilePath));
				FileSystem fs = path.getFileSystem(this.job);
				FileStatus fileStatus = fs.getFileStatus(path);
				long length = fileStatus.getLen();
				NetworkTopology clusterMap = new NetworkTopology();
				BlockLocation[] blkLocations = fs.getFileBlockLocations(fileStatus, 0, length);
				String[] splitHosts = this.textParse.getSplitHosts(blkLocations, 0, length, clusterMap);
				FileSplit fileSplit = new FileSplit(new Path[] { path }, new long[] { 0 }, new long[] { length }, null,
						null, splitHosts);
				this.updateMRDataLog(MRJobMapDataLog.STATUS_SUCCESS, perFileRecordCount, perFileOutCount, fileSplit,
						MRJobMapDataLog.DATATYPE_TXT);
			}
		} catch (Exception e) {
			// 异常设置任务节点失败
			if (this.isNeedZKMonitor && null != this.monitorZKNode) {
				try {
					this.monitorZKNode.setZKNodeData(MRWatcher.FAILED.getBytes());
				} catch (Exception e1) {
					String msg = "set zookeeper taskNode FAILED value exception!";
					MRLog.systemOut(msg + StringUtil.stringifyException(e));
				}
			}

			// 输出日志信息
			String msg = "input value=>>" + value == null ? "" : value.getRow().toString() + " exception:" +
					StringUtil.stringifyException(e);
			MRLog.systemOut(msg + StringUtil.stringifyException(e));
			e.printStackTrace();
			throw new IOException(e);
		} finally {
			// 关闭输出资源
			frt.closeOutputSource(lstOut);
			// 关闭输入资源
			frt.closeInputSource(isrp);
		}
	}

	public String getLocalHDFSFile(List<OutputSourceRecordPO> lstOutputSource) {
		for (OutputSourceRecordPO outputSourcePO : lstOutputSource) {
			if (Contant.PROTOCOL_LOCALHDFS.equals(outputSourcePO.getDataSource().getProtocol())) {
				String tmpfilePath = outputSourcePO.getPath();
				String fileNewPath = outputSourcePO.getNewPath();
				return (fileNewPath == null || fileNewPath.trim().length() <= 0) ? tmpfilePath : fileNewPath;
			}
		}
		return null;
	}

	long perFileRecordCount = 0;

	@SuppressWarnings({ "rawtypes", "unchecked", "unused" })
	private void parseFile(Context context, List<OutputSourceRecordPO> lstOutputSource, long lineCount)
			throws IOException, InterruptedException {
		FSDataOutputStream fsos = null;
		if (parseLocalFilePath == null)
			return;
		// long fileSize = HDFSUtils.getFileSize(this.job, filePath);
		String fileName = parseLocalFilePath == null ? null : parseLocalFilePath.substring(
				parseLocalFilePath.lastIndexOf("/") + 1, parseLocalFilePath.length());

		MRLog.systemOut("step1===>        outPath=" + parseLocalFilePath + ", fileName=" + fileName);
		if (null == fileName || fileName.trim().length() <= 0 || null == parseLocalFilePath ||
				parseLocalFilePath.trim().length() <= 0) {
			return;
		}

		// 实际运行标识
		if (!this.isRun) {// 新增日志
			MRLOG.addJobMapRun(this.taskId, this.jobLogId, this.totalRecordCount, this.outCount, new Date(),
					new Date(), this.runflag, "start");
			this.isRun = true;
		}

		Path path = new Path(StringUtils.unEscapeString(parseLocalFilePath));
		FileSystem fs = path.getFileSystem(this.job);

		FileStatus fileStatus = fs.getFileStatus(path);
		long length = fileStatus.getLen();
		NetworkTopology clusterMap = new NetworkTopology();
		BlockLocation[] blkLocations = fs.getFileBlockLocations(fileStatus, 0, length);
		String[] splitHosts = this.textParse.getSplitHosts(blkLocations, 0, length, clusterMap);
		this.filetype = getFileType(path);
		MRLog.systemOut("step2===>        this.filetype=" + this.filetype);
		FileSplit fileSplit = new FileSplit(new Path[] { path }, new long[] { 0 }, new long[] { length }, null, null,
				splitHosts);
		String fileterFilePath = "";
		if (null != this.filterFileRootPath && this.filterFileRootPath.trim().length() > 0) {
			fileterFilePath = this.filterFileRootPath + "_" + HDFSUtils.getFilterFileName(this.taskId, fileName);
			fsos = HDFSUtils.getFileOStream(this.ftpconf.getConf(), fileterFilePath);
		}

		// this.initMRDataLog(fileSplit, dataSourceId);
		MRLog.systemOut("step3===>        fileterFilePath=" + fileterFilePath);
		perFileRecordCount = 0;
		this.perFileOutCount = 0;// 重置单个文件有效输出记录数

		switch (this.filetype) {// 判断文件类型
		case 0:// 文本
			MRLog.systemOut("step4===>        filetype is txt file");
			RecordReader<LongWritable, Text> textReader = new LineRecordReader(this.job, fileSplit);
			while (textReader.nextKeyValue()) {
				DBRecord dbrecord[] = this.textParse.parseToDBRecord(textReader.getCurrentValue());
				if (dbrecord == null) {
					continue;
				}
				for (int i = 0; i < dbrecord.length; i++) {
					perFileRecordCount++;
					this.parseRecord(context, dbrecord[i], fsos, parseLocalFilePath);
				}
			}
			this.updateMRDataLog(MRJobMapDataLog.STATUS_SUCCESS, perFileRecordCount, perFileOutCount, fileSplit,
					MRJobMapDataLog.DATATYPE_TXT);
			break;
		case 1:// sequence
			MRLog.systemOut("step4===>        filetype is sequence file");
			RecordReader seqReader = new SequenceFileRecordReader(job, fileSplit);

			while (seqReader.nextKeyValue()) {
				DBRecord dbrecord[] = null;
				if (seqReader.getCurrentValue() instanceof Text) {
					dbrecord = this.textParse.parseToDBRecord((Text) seqReader.getCurrentValue());
				}
				if (dbrecord == null) {
					continue;
				}
				for (int i = 0; i < dbrecord.length; i++) {
					perFileRecordCount++;
					this.parseRecord(context, dbrecord[i], fsos, parseLocalFilePath);
				}
			}
			this.updateMRDataLog(MRJobMapDataLog.STATUS_SUCCESS, perFileRecordCount, perFileOutCount, fileSplit,
					MRJobMapDataLog.DATATYPE_SEQ);
			break;
		case 2:// rcfile
			MRLog.systemOut("step4===>        filetype is rcfile file");
			RecordReader<LongWritable, BytesRefArrayWritable> rcReader = new RCFileRecordReader(job, fileSplit);
			// LongWritable rckey = (LongWritable) rcReader.createKey();
			// BytesRefArrayWritable rcvalue = (BytesRefArrayWritable)
			// rcReader.createValue();
			while (rcReader.nextKeyValue()) {
				DBRecord dbrecord = this.rcParse.parseToDBRecord(rcReader.getCurrentValue());
				perFileRecordCount++;
				this.parseRecord(context, dbrecord, fsos, parseLocalFilePath);
			}
			this.updateMRDataLog(MRJobMapDataLog.STATUS_SUCCESS, perFileRecordCount, perFileOutCount, fileSplit,
					MRJobMapDataLog.DATATYPE_RCF);
			break;
		default:
			break;
		}
	}

	/**
	 * 获得文件格式
	 * 
	 * @param fs
	 * @param path
	 * @param job2
	 * @return
	 * @throws IOException
	 */
	private int getFileType(Path path) throws IOException {
		try {
			if (this.rcParse.validateRCFile(this.job, path)) {
				return 2;
			}
		} catch (Exception e) {
		}
		try {
			if (this.textParse.validateSeqFile(this.job, path)) {
				return 1;
			}
		} catch (Exception e) {
		}
		return 0;
	}

	protected void parseRecord(Context context, DBRecord record, FSDataOutputStream fsos, Object filePath)
			throws IOException, InterruptedException {
		this.totalRecordCount++;
		// 日志
		if (this.totalRecordCount % this.perInputNumber == 0) {
			String msg = "input:" + this.totalRecordCount + " row.";
			MRLog.debug(LOG, msg);
			context.setStatus(msg);
			MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_DEBUG, new Date(), msg);
		}
		// 打印日志记录
		debugInfo("输入:" + record.toString());
		// 插件处理
		List<Map<String, Object>> listVal = IRemotePlugin.mapPlugin(this.remotePlugin, record.getRow());
		if (listVal == null || listVal.size() == 0) {
			debugInfo("该记录为非法");
			return;
		}
		for (int x = 0; x < listVal.size(); x++) {
			DBRecord dbrecord = new DBRecord(listVal.get(x));
			dbrecord.setStatus(record.isStatus());
			// 高级应用处理
			if (this.mrSeniorApply != null) {
				for (int j = 0; j < this.mrSeniorApply.length; j++) {
					if (null != this.mrSeniorApply[j]) {
						this.mrSeniorApply[j].apply(dbrecord, filePath);
					}
				}
			}

			// 无效
			if (!dbrecord.isStatus()) {
				HDFSUtils.write(fsos, dbrecord.toString());
				return;
			}

			if (this.inputMapEnd) {
				// 直接输出
				DBRecord _record = new DBRecord();
				_record.getRow().putAll(this.outColumnDefaultValue);// 优先设置默认值（输出的默认值）
				_record.addChange(dbrecord, this.destFieldNames, this.destRelSrcFieldNames);
				IRemotePlugin.beforConvertRow(this.remotePlugin, _record);
				context.write(_record, null);

				// 打印日志记录
				this.debugInfo("输出:" + _record.toString());
			} else {
				DBRecord[] gps = dbrecord.splitGroup(this.destFieldNames, this.destRelSrcFieldNames,
						this.groupFieldMethod);
				IRemotePlugin.beforConvertRow(this.remotePlugin, gps[0]);
				IRemotePlugin.beforConvertRow(this.remotePlugin, gps[1]);
				context.write(gps[0], gps[1]);
				// 打印日志记录
				this.debugInfo("输出: 分组信息=>" + gps[0].toString() + " 统计信息=>" + gps[0].toString());
			}
			this.outCount++;
			this.perFileOutCount++;

			// 日志
			if (this.outCount % this.perOutputNumber == 0) {
				String msg = "output:" + this.totalRecordCount + " row.";
				MRLog.debug(LOG, msg);
				context.setStatus(msg);
				MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_DEBUG, new Date(), msg);
			}
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		this.job = context.getConfiguration();
		InputSplit inputSplit = context.getInputSplit();
		this.fileSplit = (FTPInputSplit) inputSplit;

		this.setJobId(job.getLong(MRConfiguration.SYS_JOB_ID, 0));
		this.setSysJobId(job.get("mapred.job.id"));
		this.taskId = job.get("mapred.tip.id"); // 获取任务id

		// 解析地址
		this.ftpconf = new FTPConfiguration(job);
		int fileDownftptype = job.getInt(MRConfiguration.INTERNAL_SYS_FILE_DOWN_OR_DO_FTPTYPE, -1);
		int fileUpftptype = job.getInt(MRConfiguration.INTERNAL_SYS_FILE_UP_FTPTYPE, -1);
		MRLog.systemOut(this.getDonwTypeMsg(fileDownftptype, fileUpftptype));

		// 设置zk监听参数
		this.isNeedZKMonitor = true;
		this.monitor(this.ftpconf);

		// 初始化plugin对象
		this.remotePlugin = IRemotePlugin.configurePlugin(context, this);
		if (MRConfiguration.INTERNAL_SYS_FILE_DOWN_OR_DO_FTPTYPE_DOWN.equals(String.valueOf(fileDownftptype)) ||
				MRConfiguration.INTERNAL_SYS_FILE_UP_FTPTYPE_UP.equals(String.valueOf(fileUpftptype))) {
			return;
		}

		/**
		 * 获取解析文本的参数
		 */
		this.isParse = true;
		MRLog.systemOut("conf====>" + "this.isParse=" + this.isParse);
		FileConfiguration dbconf = new FileConfiguration(job);
		this.taskId = dbconf.getConf().get("mapred.tip.id"); // 获取任务id
		this.jobLogId = dbconf.getJobLogId(); // 获取job的日志id
		this.perInputNumber = dbconf.getJOBLogMapInputPerRecordNumber(); // 实时记录输入数据量
		this.perOutputNumber = dbconf.getJOBLogMapOutputPerRecordNumber(); // 实时记录输入数据量

		this.inputMapEnd = dbconf.getInputMapEnd();// 是否map直接输出结果
		this.srcFieldNames = dbconf.getInputFieldNames();// 输入字段
		this.rowSplitChars = dbconf.getInputFileRowsSplitChars();// 行分隔符
		this.fieldSplitChars = dbconf.getInputFileFieldSplitChars();// 列分隔符
		String[] targetFieldNames = dbconf.getOutputFieldNames();// 输出
		this.groupFieldMethod = dbconf.getRelatedGroupFieldMethod();// 目标到源映射及方法

		this.debugRowNum = dbconf.getJobLogDebugRowNum();
		this.debug = dbconf.getJobLogDebug();

		// 若未指定输出字段，直接将输入字段作为输出字段
		if (null == targetFieldNames) {
			targetFieldNames = this.srcFieldNames.clone();
		}
		this.outColumnDefaultValue = StringUtil.decodeOutColumnDefaultValue(this.ftpconf.getOutputColumnDefaultValue());// 获取直接输出的输出字段的默认值

		// 打印级别1
		if (this.debug >= MRConfiguration.INTERNAL_JOB_LOG_DEBUG_CONF_INFO) {
			StringBuffer orginConf = new StringBuffer();
			orginConf.append("\n实时记录输入数据量:" + this.perInputNumber + "\n");
			orginConf.append("实时记录输入数据量:" + this.perOutputNumber + "\n");
			orginConf.append("输入字段:" + StringUtil.toString(this.srcFieldNames, null) + "\n");
			orginConf.append("输出字段:" + StringUtil.toString(targetFieldNames, null) + "\n");
			orginConf.append("目标到源映射及方法:" + StringUtil.toString(this.groupFieldMethod, null) + "\n");
			MRLog.systemOut(orginConf.toString());
		}

		Set<String> setSrcFieldNames = new HashSet<String>();
		CollectionUtils.addAll(setSrcFieldNames, this.srcFieldNames);

		Set<String> setTargetFieldNames = new HashSet<String>();
		CollectionUtils.addAll(setTargetFieldNames, targetFieldNames);

		if (this.inputMapEnd) {
			// map直接输出拆分字段
			this.noReduceAnalyzeConfig(job, setSrcFieldNames, setTargetFieldNames);
		} else {
			this.analyzeConfig(job, setSrcFieldNames, setTargetFieldNames);
		}

		this.textParse = new TextParse(this.srcFieldNames, this.fieldSplitChars, this.rowSplitChars, this.taskId);
		this.rcParse = new RCFileParse(this.srcFieldNames);

		// 初始化日志信息
		MRLog.getInstance().setConf(dbconf);

		// 设置zk监听参数
		this.isNeedZKMonitor = dbconf.isZKMonitor();
		this.monitor(dbconf);

		// 初始化高级应用对象
		this.senior(dbconf);

		// 获取过滤数据存放的根路径
		this.filterFileRootPath = this.getFilterRootPath(dbconf);

		// 打印级别2
		if (this.debug >= MRConfiguration.INTERNAL_JOB_LOG_DEBUG_DECONF_INFO) {
			StringBuffer deConf = new StringBuffer();
			deConf.append("\n任务id:" + this.taskId + "\n");
			deConf.append("JOB的日志ID:" + this.jobLogId + "\n");
			deConf.append("是否map直接输出结果:" + this.inputMapEnd + "\n");
			deConf.append("处理后的输入字段:" + StringUtil.toString(this.destFieldNames, null) + "\n");
			deConf.append("处理后的输出字段:" + StringUtil.toString(this.destRelSrcFieldNames, null) + "\n");
			deConf.append("处理后的目标到源映射及方法:" + StringUtil.toString(this.groupFieldMethod, null) + "\n");
			MRLog.systemOut(deConf.toString());
		}
		// switch (this.filetype) {
		// case 0:
		// case 1:
		// this.textParse = new TextParse(this.srcFieldNames,
		// this.fieldSplitChars, this.rowSplitChars, this.taskId);
		// break;
		// case 2:
		// this.rcParse = new RCFileParse(this.srcFieldNames);
		// break;
		// default:
		// break;
		// }
	}

	private String getDonwTypeMsg(int fileDownftptype, int fileUpftptype) {
		switch (fileDownftptype) {
		case 0:
			return "传输类型：0 下载";
		case 1:
			return "传输类型：1 下载并处理";
		case 2:
			return "传输类型：2 处理";
		default:
			break;
		}

		switch (fileUpftptype) {
		case 0:
			return "传输类型：0 上传";
		default:
			break;
		}

		return "";
	}

	@Override
	public void senior(MRConfiguration conf) {
		if (null == conf) {
			return;
		}

		String sconf = conf.getInputMapSeniorConf();
		if (null == sconf || sconf.length() <= 0) {
			return;
		}

		this.mrSeniorApply = MRSeniorUtil.getMRSeniorApplies(sconf, conf.getConf(), 0);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		IRemotePlugin.closePlugin(this.remotePlugin);
	}

	/**
	 * 分析配置信息
	 * 
	 * @param job
	 *            job对象
	 * @param setSrcFieldNames
	 *            输入字段
	 * @param setTargetFieldNames
	 *            输出字段
	 */
	private void analyzeConfig(Configuration job, Set<String> setSrcFieldNames, Set<String> setTargetFieldNames) {
		this.destFieldNames = new String[this.groupFieldMethod.length];
		this.destRelSrcFieldNames = new String[this.groupFieldMethod.length];
		for (int i = 0; i < this.groupFieldMethod.length; i++) {
			String[] tmp = this.groupFieldMethod[i].split(":");
			if (tmp.length != 3) {
				String msg = "分组计算中目标到源映射及方法配置不正确:" + job.get(MRConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL);
				MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_ERROR, new Date(), msg);
				throw new RuntimeException(msg);
			}

			if (!setTargetFieldNames.contains(tmp[0])) {
				String msg = "配置错误,目标字段" + tmp[0] + "不存在于输出字段 :" +
						job.get(MRConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL);
				MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_ERROR, new Date(), msg);
				throw new RuntimeException(msg);
			}

			if (!setSrcFieldNames.contains(tmp[1])) {
				String msg = "配置错误,源字段" + tmp[1] + "不存在输入字段 :" +
						job.get(MRConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL);
				MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_ERROR, new Date(), msg);
				throw new RuntimeException(msg);
			}

			this.destFieldNames[i] = tmp[0];
			this.destRelSrcFieldNames[i] = tmp[1];
			this.groupFieldMethod[i] = tmp[2];
		}
	}

	/**
	 * Map直接输出的分析配置信息
	 * 
	 * @param job
	 *            job对象
	 * @param setSrcFieldNames
	 *            输入字段
	 * @param setTargetFieldNames
	 *            输出字段
	 */
	private void noReduceAnalyzeConfig(Configuration job, Set<String> setSrcFieldNames, Set<String> setTargetFieldNames) {
		if (null == this.groupFieldMethod) {
			return;
		}

		if (this.groupFieldMethod.length <= 0) {
			String msg = "目标到源字段配置为空:" + job.get(MRConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL);
			MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_ERROR, new Date(), msg);
			throw new RuntimeException(msg);
		}

		this.destFieldNames = new String[this.groupFieldMethod.length];
		this.destRelSrcFieldNames = new String[this.groupFieldMethod.length];
		for (int i = 0; i < this.groupFieldMethod.length; i++) {
			String tts[] = this.groupFieldMethod[i].split(":");
			if (tts.length != 2) {
				String msg = "目标到源字段配置不正确:" + job.get(MRConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL);
				MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_ERROR, new Date(), msg);
				throw new RuntimeException(msg);
			}

			if (!setTargetFieldNames.contains(tts[0])) {
				String msg = "配置错误,目标字段" + tts[0] + "不存在于输出字段 :" +
						job.get(MRConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL);
				MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_ERROR, new Date(), msg);
				throw new RuntimeException(msg);
			}

			if (!setSrcFieldNames.contains(tts[1])) {
				String msg = "配置错误,源字段" + tts[1] + "不存在输入字段 :" +
						job.get(MRConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL);
				MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_ERROR, new Date(), msg);
				throw new RuntimeException(msg);
			}
			this.destFieldNames[i] = tts[0];
			this.destRelSrcFieldNames[i] = tts[1];
		}
	}

	@Override
	public void monitor(MRConfiguration conf) {
		String zkTaskPath = null;
		if (this.isNeedZKMonitor) {
			String zkAddress = conf.getZKAddress();
			String zkRootPath = conf.getZKFTPPath();
			zkTaskPath = zkRootPath + "/" + this.taskId;
			this.monitorZKNode = new MonitorZKNode(this.taskId, zkTaskPath, zkAddress);

			String msg = "FileMapper monitor task nodePath is:" + zkTaskPath;
			MRLog.systemOut(msg);
			if (this.isParse) {
				MRLog.infoZK(LOG, msg);
				MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_INFO, new Date(), msg);
			}
		}
	}

	/**
	 * 获取过滤数据存放的根路径
	 * 
	 * @param conf
	 */
	private String getFilterRootPath(MRConfiguration conf) {
		if (null == this.mrSeniorApply || this.mrSeniorApply.length == 0) {
			return null;
		}

		String outputPath = null;
		boolean flag = false;
		for (int j = 0; j < this.mrSeniorApply.length; j++) {
			if (null != this.mrSeniorApply[j] || mrSeniorApply[j] instanceof MRSeniorOperationFilter) {
				flag = true;
				break;
			}
		}

		if (!flag) {
			return null;
		}

		try {
			if (conf.isOutputMapSeniorFilterIsOutput()) {
				outputPath = conf.getOutputMapSeniorFilterOutputPath();
				if (null != outputPath) {
					FileSystem fs = FileSystem.get(conf.getConf());
					Path path = new Path(outputPath);
					if (!fs.exists(path)) {
						fs.mkdirs(path);
						return outputPath;
					} else {
						if (fs.isFile(path)) {
							return conf.getMapredInputDir();
						} else if (fs.getFileStatus(path).isDir()) {
							return outputPath;
						}
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return outputPath;
	}

	/**
	 * 更新日志信息
	 * 
	 * @param reporter
	 */
	private void updateMRDataLog(int status, long recordCount, long outCount, FileSplit fileSplit, int dataType) {
		String path = fileSplit.getPath().toUri().toString();
		MRJobMapDataLog dataLog = new MRJobMapDataLog();
		dataLog.setDataType(dataType);
		dataLog.setEndTime(StringUtil.dateToString(new Date(), StringUtil.DATE_FORMAT_TYPE1));
		dataLog.setFailCount(recordCount - outCount);
		dataLog.setStatus(status);
		dataLog.setSuccessCount(outCount);
		dataLog.setTotalCount(this.totalRecordCount);
		dataLog.setFilterPath(this.filterFileRootPath + "/" + HDFSUtils.getFilterFileName(this.taskId, path));
		dataLog.setId(this.fileDataLogId);
		MRLOG.updateMRJobMapDataLogFTP(dataLog);
	}

	/**
	 * 打印记录的日志信息
	 * 
	 * @param value
	 *            日志信息
	 */
	private void debugInfo(String value) {
		// 打印级别3
		if (this.debug == MRConfiguration.INTERNAL_JOB_LOG_DEBUG_RECORD) {
			if (this.debugRowNum >= this.totalRecordCount) {
				MRLog.consoleDebug(LOG, value);
			}
		}
	}

	public long getJobId() {
		return jobId;
	}

	public void setJobId(long jobId) {
		this.jobId = jobId;
	}

	public String getSysJobId() {
		return sysJobId;
	}

	public void setSysJobId(String sysJobId) {
		this.sysJobId = sysJobId;
	}

}