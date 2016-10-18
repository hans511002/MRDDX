package com.ery.hadoop.mrddx.remote;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.IFileTransfer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.tools.tar.TarInputStream;

import com.enterprisedt.net.ftp.FTPException;
import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.log.MRColDetailLog;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.mode.MRFileListPO;
import com.ery.hadoop.mrddx.remote.plugin.IRemotePlugin;
import com.ery.hadoop.mrddx.util.HDFSUtils;
import com.ery.hadoop.mrddx.util.StringUtil;

/**
 * @author Administrator
 * 
 */
public class FileRecordTrans implements Configurable {

	public static final Log LOG = LogFactory.getLog(FTPMaper.class);
	private CompressType compressType;
	private IRemotePlugin remotePlugin;
	private int connectMax;
	private int connectTime;
	// 无效的记录数
	private long recordInvalidCount;

	public long FileSize = 0;
	public long lineCount = 0;
	Context context;
	Configuration conf;
	InputSourceRecordPO isrp;

	String fileEncodeing = "utf-8";
	boolean fileEncodeingIsUTF8 = true;

	public FileRecordTrans(Configuration conf) {
		this.setConf(conf);
	}

	public FileRecordTrans(Context context) {
		this.context = context;
		this.setConf(context.getConfiguration());
	}

	void plusPlugin(int len) {
		FileSize += len;
		lineCount++;
		if (context != null) {
			if ((lineCount % 10000) == 0) {
				context.setStatus("文件:" + isrp.getPath() + " 下载：" + lineCount + "行,大小：" + FileSize);
			}
		}
	}

	public boolean inputToOutput(Configuration conf, InputSourceRecordPO isrp, List<OutputSourceRecordPO> lstOsrp)
			throws IOException, InterruptedException {
		this.setConf(conf);
		this.connectMax = StringUtil.stringToInt(conf.get(MRConfiguration.CONNECT_MAX), 0);
		this.connectTime = StringUtil.stringToInt(conf.get(MRConfiguration.CONNECT_TIME), 0);
		this.isrp = isrp;

		// 初始传输
		this.inputToOutputDo(conf, isrp, lstOsrp);

		// 处理重传
		int retryTime = 0;
		boolean result = true;
		while (this.checkInOutFailed(isrp, lstOsrp)) {// 检查输入或者输出中存在失败的状态,
														// 若存在，重试
			retryTime++;
			MRLog.systemOut("After sleep " + connectTime + "retry, retry times:" + retryTime);

			// 睡眠时间
			try {
				Thread.sleep(this.connectTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			if (isrp.getStatus() != InputSourceRecordPO.STATUS_SUCCESS && isrp.getRetrytimes() < this.connectMax) {
				isrp.setRetrytimes(isrp.getRetrytimes() + 1);
				MRLog.systemOut("Because the input is failure, maxRetryTime=" + this.connectMax);
			}

			if (isrp.getRetrytimes() >= this.connectMax) {
				MRLog.systemOut("Because the input is failure, More than maximum retries " + this.connectMax);
				result = false;
				break;
			}

			List<OutputSourceRecordPO> lstfailOsrp = new ArrayList<OutputSourceRecordPO>();
			for (OutputSourceRecordPO osrp : lstOsrp) {
				if (osrp.getStatus() == OutputSourceRecordPO.STATUS_SUCCESS) {
					continue;
				}

				if (osrp.getRetrytimes() < this.connectMax) {
					lstfailOsrp.add(osrp);
					osrp.setRetrytimes(osrp.getRetrytimes() + 1);
					MRLog.systemOut("Because the input is failure," + osrp.toString() + " maxRetryTime=" +
							this.connectMax);
				} else { // 输出端，存在一个传输失败的情况，不影响其他输出，但是整个任务状态为失败
					result = false;
				}
			}

			if (lstfailOsrp.size() <= 0) {
				break;
			}

			this.inputToOutputDo(conf, isrp, lstfailOsrp);
		}

		return result;
	}

	/**
	 * 检查输入或者输出中存在失败的状态
	 * 
	 * @param isrp
	 * @param lstOsrp
	 * @return true: 存在失败的
	 */
	private boolean checkInOutFailed(InputSourceRecordPO isrp, List<OutputSourceRecordPO> lstOsrp) {
		if (isrp.getStatus() != InputSourceRecordPO.STATUS_SUCCESS) {
			return true;
		}

		for (OutputSourceRecordPO osrp : lstOsrp) {
			if (osrp.getStatus() != OutputSourceRecordPO.STATUS_SUCCESS) {
				return true;
			}
		}

		return false;
	}

	/**
	 * 数据传输(输入到输出)
	 * 
	 * @param conf
	 * @param lstIsrp
	 *            输入
	 * @param lstOsrp
	 *            输出队列
	 * @throws IOException
	 */
	public boolean inputToOutputDo(Configuration conf, InputSourceRecordPO isrp, List<OutputSourceRecordPO> lstOsrp)
			throws IOException, InterruptedException {
		if (null == isrp) {
			return false;
		}

		// 输入：获取参数
		isrp.setStatus(0);// 初始化状态
		String inFilePath = isrp.getPath();// 输入文件路径
		this.compressType = StringUtil.getCompressType(inFilePath); // 获取文件压缩类型
		MRLog.systemOut(MRLog.get2Space() + "输入文件压缩类型:" + this.compressType.toString());
		FTPDataSourcePO inFtpds = isrp.getDataSource();
		String inProtocl = inFtpds.getProtocol();

		// 输入：获取系统资源
		Object inSystem = SourceUtil.initServer(inFtpds);
		isrp.setInputSourceSystem(inSystem);
		if (null == inSystem) {
			isrp.setStatus(InputSourceRecordPO.STATUS_GET_SYSTEMSOURCE_FAIL);
			MRLog.systemOut(MRLog.get2Space() + "get Input Source failed");
			return false;
		}

		// 输入：获取输入流（只获取一次）
		InputStream is = null;
		try {
			is = this.getInputStream(inSystem, inProtocl, inFilePath, conf);
			if (this.isTextFile()) {// 文本读取
				is = this.getCompressInputStream(is);
				MRLog.systemOut(MRLog.get2Space() + " text read need getCompressInputStream compressType=" +
						this.compressType + "  is=" + is);
			} else {
				MRLog.systemOut(MRLog.get2Space() + " not text read  compressType=" + this.compressType + "  is=" + is);
			}
			if (null == is) {// 输入流为空
				MRLog.systemOut(MRLog.get2Space() + "get Input Stream failed");
				isrp.setStatus(InputSourceRecordPO.STATUS_GET_INPUTSTREAM_FAIL);
				return false;
			}
		} catch (Exception e) {// 输入流异常
			e.printStackTrace();
			MRLog.systemOut(MRLog.get2Space() + "get Input Stream exception");
			isrp.setStatus(InputSourceRecordPO.STATUS_GET_INPUTSTREAM_FAIL);
			return false;
		}

		List<OutputStream> lstOS = new ArrayList<OutputStream>();
		Map<OutputStream, OutputSourceRecordPO> mapOSOutput = new HashMap<OutputStream, OutputSourceRecordPO>();
		int i = 0;
		// 获取目标输出流
		for (OutputSourceRecordPO osrp : lstOsrp) {
			i++;
			MRLog.systemOut(MRLog.get2Space() + "第<" + i + ">个输出文件");
			MRLog.systemOut(MRLog.get4Space() + "初始化输出服务资源：" + osrp.getDataSource().getMetaInfo());
			if (osrp.isTranSuccess()) { // 传输成功，不再执行传输
				MRLog.systemOut(MRLog.get4Space() + "已经传输成功，无需重传");
				continue;
			}
			// 输出：获取参数
			osrp.setStatus(0);// 初始化状态
			FTPDataSourcePO outFtpds = osrp.getDataSource();
			String outProtocol = outFtpds.getProtocol();
			// 输出: 获取系统资源
			Object outSystem = SourceUtil.initServer(outFtpds);
			MRLog.systemOut(MRLog.get2Space() + "  outSystem=" + outSystem);

			osrp.setOutputSourceSystem(outSystem);
			if (null == outSystem) {
				MRLog.systemOut(MRLog.get4Space() + "初始化服务资源结果：失败");
				osrp.setStatus(OutputSourceRecordPO.STATUS_GET_SYSTEMSOURCE_FAIL);
				continue;
			}
			MRLog.systemOut(MRLog.get4Space() + "初始化服务资源结果：成功");

			// 输出: 判断重新下载或者初始下载
			FTPLogPO ftpLogPO = MRLog.getInstance().getJobRemoteFileCreateDate(isrp.getColId(), osrp.getColLogId(),
					isrp.getSourceOnlyFlag(), osrp.getSourceOnlyFlag());
			if (null != ftpLogPO) {
				osrp.setDelFile(true);// 需要再传输，则标示删除之前输出的文件
			}
			// 输出:获取文件流
			OutputStream os = null;
			try {
				MRLog.systemOut(MRLog.get4Space() + "开始获取输出流");
				if (!this.isTextFile() && this.compressType != CompressType.NONE) {
					osrp.setCompress(0);
				}
				os = this.getOutPutStream(outProtocol, osrp.getPath(), outSystem, os, osrp.isDelFile(),
						osrp.getCompress(), conf, osrp);
				if (this.isTextFile()) {
					if (os != null && osrp.getCompress() != 1) {// 若输出已选择压缩，则不再重复压缩，否则，保持原来文件的类型
						os = this.getCompressOutputStream(os);
					}
				}
				MRLog.systemOut(MRLog.get2Space() + "  os=" + os);
			} catch (Exception e) {// 单个输出流失败，继续传输
				e.printStackTrace();
				MRLog.systemOut(MRLog.get4Space() + "get output Stream exception");
				osrp.setStatus(OutputSourceRecordPO.STATUS_GET_OUTPUTSTREAM_FAIL);
				continue;
			}

			if (os != null) {
				MRLog.systemOut(MRLog.get4Space() + "获取输出流:成功");
				lstOS.add(os);
				mapOSOutput.put(os, osrp);
			}
		}

		// 输入和输出：数据传输
		List<OutputStream> lstErrorOS = new ArrayList<OutputStream>();
		long destFileSize = 0;
		try {
			if (lstOS.size() > 0) {
				MRLog.systemOut(MRLog.get2Space() + "开始传输 compressType=" + compressType + "  isTextFile=" +
						this.isTextFile());
				if (!isTextFile()) {
					MRLog.systemOut(MRLog.get2Space() + " binary read compressType=" + compressType + "  isTextFile=" +
							this.isTextFile());
					destFileSize = writeBinaryFile(is, lstOS, mapOSOutput, lstErrorOS);
					if (lstOS.size() > 1)
						this.FileSize = destFileSize;
				} else {
					if (this.compressType == CompressType.TAR || this.compressType == CompressType.TARGZ) {
						// tar.gz 包压缩
						destFileSize = this.targzWrite(is, lstOS, mapOSOutput, lstErrorOS);
					} else {// 默认传输
						destFileSize = this.defaultWrite(is, lstOS, mapOSOutput, lstErrorOS);
					}
				}
				MRLog.systemOut(MRLog.get2Space() + "结束传输 destFileSize=" + FileSize);
				// // 清空缓存
				// for (OutputStream outs : lstOS) {
				// if (lstErrorOS.contains(outs)) {
				// continue;
				// }
				// this.flush(outs);
				// }
			}
			return true;
		} catch (InputIOException ie) {
			System.out.println("异常-------------");
			ie.printStackTrace();
			MRLog.systemOut("Abnormal transfer");
			return false;
		} finally {
			// 关闭输入流
			try {
				if (null != is) {
					is.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

			// 清空输出流的缓存
			for (int c = 0; c < lstOsrp.size(); c++) {
				OutputSourceRecordPO osrp = lstOsrp.get(c);
				// if (lstOS.get(c) != null && lstOS.size() > c &&
				// lstOsrp.get(c) != null) {
				// OutputStream os = lstOS.get(c);
				// if (os instanceof GZIPOutputStream) {
				// GZIPOutputStream gos = (GZIPOutputStream) os;
				//
				// }
				// } else {
				// }
				osrp.setSize(destFileSize);
			}
			for (OutputStream os : lstOS) {
				if (null != os) {
					try {
						os.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}

			// 记录日志
			MRLog.systemOut(MRLog.get2Space() + "更新记录日志信息");
			this.recordLog(conf, isrp, lstOsrp, this.recordInvalidCount);
			// 关闭资源
			MRLog.systemOut(MRLog.get2Space() + "关闭输出资源");
			this.closeOutputSource(lstOsrp);
			MRLog.systemOut(MRLog.get2Space() + "关闭输入资源");
			this.closeInputSource(isrp);
		}
	}

	/**
	 * 记录日志
	 * 
	 * @param conf
	 * @param isrp
	 * @param lstOsrp
	 * @param colLogId
	 * @param lastModifyTime
	 */
	private void recordLog(Configuration conf, InputSourceRecordPO isrp, List<OutputSourceRecordPO> lstOsrp,
			long recordInvalidCount) {
		// 记录日志信息
		boolean instatus = isrp.getStatus() == InputSourceRecordPO.STATUS_SUCCESS;
		int status = 0;
		for (OutputSourceRecordPO osrp : lstOsrp) {
			if (osrp.isTranSuccess()) { // 传输成功，不再执行传输
				continue;
			}
			boolean outstatus = osrp.getStatus() == OutputSourceRecordPO.STATUS_SUCCESS;
			status = (instatus && outstatus) ? FTPLogPO.STATUS_SUCCESS : FTPLogPO.STATUS_FAIL;
			this.updateLog(isrp, osrp, status, osrp.getColLogId(), recordInvalidCount);
		}
	}

	boolean isTextFile() {
		FTPConfiguration c = new FTPConfiguration(conf);
		if ((c.getFtpDownCollectDataType() == 0 // 文本文件
				&& c.getFtpDownCollectType() == 0// 下载
				) ||
				(c.getFtpUpCollectDataType() == 0 // 文本文件
				&& c.getFtpUpCollectType() == 1// 上传
				)) {
			return true;
		}
		return false;
	}

	FTPConfiguration ftpconf = null;
	// 下载参数
	public boolean isParse = false;
	public boolean isRunLog = false;

	public FTPMaper map = null;
	long recordCount = 0;
	long perFileOutCount = 0;
	FSDataOutputStream parseFilterfsos = null;

	private void parseTextToBase(String line) throws IOException, InterruptedException {
		if (ftpconf == null && conf != null) {
			ftpconf = new FTPConfiguration(conf);// 下载参数
			int fileDownftptype = ftpconf.getFileDownOrDoFTPType();
			int fileUpftptype = ftpconf.getFileUPFTPType();
			if (MRConfiguration.INTERNAL_SYS_FILE_DOWN_OR_DO_FTPTYPE_DOWN.equals(String.valueOf(fileDownftptype)) ||
					MRConfiguration.INTERNAL_SYS_FILE_UP_FTPTYPE_UP.equals(String.valueOf(fileUpftptype))) {
				return;
			} else {
				if (map.parseLocalFilePath == null)
					return;
				// long fileSize = HDFSUtils.getFileSize(this.job, filePath);
				String fileName = map.parseLocalFilePath.substring(map.parseLocalFilePath.lastIndexOf("/") + 1,
						map.parseLocalFilePath.length());
				String fileterFilePath = "";
				if (null != this.map.filterFileRootPath && this.map.filterFileRootPath.trim().length() > 0) {
					fileterFilePath = this.map.filterFileRootPath + "_" +
							HDFSUtils.getFilterFileName(this.map.taskId, fileName);
					parseFilterfsos = HDFSUtils.getFileOStream(this.ftpconf.getConf(), fileterFilePath);
				}
				this.isParse = true;
			}
		}
		if (map == null || !isParse) {
			return;
		}
		// 解析文件 入库

		// FSDataOutputStream fsos = null;
		// String filePath = null;
		// long fileSize = -1;
		// String fileName = null;
		// boolean flag = false;
		// int dataSourceId = -1;

		// 实际运行标识
		if (!this.isRunLog) {// 新增日志
			map.MRLOG.addJobMapRun(map.taskId, map.jobLogId, map.totalRecordCount, map.outCount, new Date(),
					new Date(), map.runflag, "start");
			this.isRunLog = true;
			MRLog.systemOut("parseTextFile===>" + isrp.getPath());
		}
		DBRecord dbrecord[] = this.map.textParse.parseToDBRecord(line);
		if (dbrecord == null) {
			return;
		}
		for (int i = 0; i < dbrecord.length; i++) {
			recordCount++;
			this.map.parseRecord(context, dbrecord[i], parseFilterfsos, map.parseLocalFilePath);
		}
	}

	/**
	 * 默认传输
	 * 
	 * @param isrp
	 * @param lstOsrp
	 * @param colLogId
	 * @param connectMax
	 * @param lstOsrpTemp
	 * @param lastModifyTime
	 * @param is
	 * @param lstOS
	 * @param mapOSOutput
	 * @param lstErrorOS
	 * @throws IOException
	 */
	private long defaultWrite(InputStream is, List<OutputStream> lstOS,
			Map<OutputStream, OutputSourceRecordPO> mapOSOutput, List<OutputStream> lstErrorOS) throws IOException,
			InterruptedException {
		if (isTextFile()) {
			BufferedReader isr = new BufferedReader(new InputStreamReader(is, this.fileEncodeing));
			String strBuff = null;
			String value = null;
			while ((strBuff = isr.readLine()) != null) {
				if (!fileEncodeingIsUTF8) {// 编码转换
					strBuff = new String(strBuff.getBytes(MRConfiguration.FILE_CONTENT_ENCODING_DEFAULT),
							MRConfiguration.FILE_CONTENT_ENCODING_DEFAULT);
				}
				if (this.remotePlugin != null) {// 插件处理
					value = null;
					value = this.remotePlugin.line(strBuff);
					if (null == value) {// 插件处理过后，被过滤掉
						this.recordInvalidCount++;
						continue;
					}
				}
				// 写数据
				boolean plus = true;
				for (OutputStream outs : lstOS) {
					this.write(strBuff + "\n", outs, lstErrorOS, mapOSOutput, plus);
					plus = false;
				}
				parseTextToBase(strBuff);
			}
			return this.FileSize;
		} else {
			return writeBinaryFile(is, lstOS, mapOSOutput, lstErrorOS);
		}
	}

	public long writeBinaryFile(InputStream is, List<OutputStream> lstOS,
			Map<OutputStream, OutputSourceRecordPO> mapOSOutput, List<OutputStream> lstErrorOS)
			throws OutputIOException, IOException {
		byte[] buf = new byte[64 * 1024];
		int len = 0;
		int nullCount = 0;
		long fz = 0;
		while ((len = is.read(buf)) >= 0) {
			if (len == 0) {
				try {
					nullCount++;
					Thread.sleep(100);
				} catch (InterruptedException e) {
				}
				if (nullCount > 100) {
					break;
				}
			}
			// 写数据
			boolean plus = true;
			for (OutputStream outs : lstOS) {
				this.write(buf, len, outs, lstErrorOS, mapOSOutput, plus);
				plus = false;
			}
			fz += len;
		}
		return fz;
	}

	/**
	 * 打包并压缩为tar.gz的传输
	 * 
	 * @param isrp
	 * @param lstOsrp
	 * @param colLogId
	 * @param connectMax
	 * @param lstOsrpTemp
	 * @param lastModifyTime
	 * @param is
	 * @param lstOS
	 * @param mapOSOutput
	 * @param lstErrorOS
	 * @throws IOException
	 */
	private long targzWrite(InputStream is, List<OutputStream> lstOS,
			Map<OutputStream, OutputSourceRecordPO> mapOSOutput, List<OutputStream> lstErrorOS) throws IOException,
			InterruptedException {
		if (isTextFile()) {
			Text text = new Text();
			LineReaders line = new LineReaders(is);
			String value = null;
			while (line.readLine(text) != 0) {
				if (fileEncodeingIsUTF8) {// 编码转换
					value = text.toString();
				} else {
					byte[] bts = new String(text.getBytes(), 0, text.getLength(), this.fileEncodeing)
							.getBytes(MRConfiguration.FILE_CONTENT_ENCODING_DEFAULT);
					value = new String(bts, MRConfiguration.FILE_CONTENT_ENCODING_DEFAULT);
				}
				if (this.remotePlugin != null) {// 插件处理
					value = null;
					value = this.remotePlugin.line(value);
					if (null == value) {// 插件处理过后，被过滤掉
						this.recordInvalidCount++;
						continue;
					}
				}
				// 写数据
				boolean fz = true;
				for (OutputStream outs : lstOS) {
					this.write(value + "\n", outs, lstErrorOS, mapOSOutput, fz);
					fz = false;
				}
				parseTextToBase(value);
			}
			return this.FileSize;
		} else {
			return writeBinaryFile(is, lstOS, mapOSOutput, lstErrorOS);
		}
	}

	private int read(InputStream is, byte[] buff) throws InputIOException {
		try {
			return is.read(buff, 0, 8192);
		} catch (IOException e) {
			e.printStackTrace();
			throw new InputIOException();
		}
	}

	private void flush(OutputStream outs) {
		try {
			outs.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void write(byte[] buff, int len, OutputStream outs, List<OutputStream> lstErrorOS,
			Map<OutputStream, OutputSourceRecordPO> mapOSOutput, boolean plus) throws OutputIOException {
		if (lstErrorOS.contains(outs)) {
			return;
		}
		try {
			if (len > 0) {
				if (plus)
					plusPlugin(len);
				outs.write(buff, 0, len);
			}
		} catch (IOException e) {
			lstErrorOS.add(outs);
			OutputSourceRecordPO output = mapOSOutput.get(outs);
			if (null != output) {
				output.setStatus(OutputSourceRecordPO.STATUS_GET_OUTPUTSTREAM_FAIL);
			}
		}
	}

	private void write(String strBuff, OutputStream outs, List<OutputStream> lstErrorOS,
			Map<OutputStream, OutputSourceRecordPO> mapOSOutput, boolean plus) throws OutputIOException {
		if (lstErrorOS.contains(outs)) {
			return;
		}

		try {
			byte[] val = strBuff.getBytes();
			if (plus)
				plusPlugin(val.length);
			outs.write(val);
		} catch (IOException e) {
			e.printStackTrace();
			lstErrorOS.add(outs);
			OutputSourceRecordPO output = mapOSOutput.get(outs);
			if (null != output) {
				output.setStatus(OutputSourceRecordPO.STATUS_GET_OUTPUTSTREAM_FAIL);
			}
		}
	}

	public void closeOutputSource(List<OutputSourceRecordPO> lstOsrp) {
		// 关闭输出资源
		for (OutputSourceRecordPO osrp : lstOsrp) {
			SourceUtil.close(osrp.getOutputSourceSystem());
			osrp.setOutputSourceSystem(null);
		}
	}

	public void closeInputSource(InputSourceRecordPO isrp) {
		// 关闭输入资源
		SourceUtil.close(isrp.getInputSourceSystem());
		isrp.setInputSourceSystem(null);
	}

	public void updateLog(InputSourceRecordPO isrp, OutputSourceRecordPO osrp, int status, long colLogId,
			long recordInvalidCount) {
		FTPLogPO log = new FTPLogPO();
		log.setColId(isrp.getColId());
		log.setRunDateTime(StringUtil.dateToString(new Date(), StringUtil.DATE_FORMAT_TYPE1));
		log.setInputFileMsg(isrp.getSourceOnlyFlag());
		log.setOutputFileMsg(osrp.getSourceOnlyFlag());
		log.setInputFileLastModifyTime(isrp.getLastModiyTime());
		log.setStatus(status);
		log.setColLogId(colLogId);
		log.setColFileDetailId(osrp.getColFileDetailId());
		log.setRecordInvalidCount(recordInvalidCount);
		MRLog.getInstance().updateJobRemoteFileMsg(log);
	}

	/**
	 * 处理输入输出详细日志
	 * 
	 * @param isrp
	 */
	public void updateColFileLog(InputSourceRecordPO isrp, List<OutputSourceRecordPO> lstOutput) {
		MRColDetailLog filelog = new MRColDetailLog();
		int delInputStatus = -1;
		if (isrp.isDelete()) {
			delInputStatus = isrp.isDeleteStatus() ? MRColDetailLog.DELETEINPUTSTATUS_SUCCESS
					: MRColDetailLog.DELETEINPUTSTATUS_FAIL;
		}
		filelog.setDeleteInputStatus(delInputStatus);

		int mvInputStatus = -1;
		if (isrp.isMove()) {
			mvInputStatus = isrp.isMovePathStatus() ? MRColDetailLog.MOVEINPUTSTATUS_SUCCESS
					: MRColDetailLog.MOVEINPUTSTATUS_FAIL;
		}
		filelog.setMoveInputStatus(mvInputStatus);

		int reInputStatus = -1;
		if (isrp.isRename()) {
			reInputStatus = isrp.isRenameRuleStatus() ? MRColDetailLog.RENAMEINPUTSTATUS_SUCCESS
					: MRColDetailLog.RENAMEINPUTSTATUS_FAIL;
		}
		filelog.setRenameInputStatus(reInputStatus);

		if (isrp.isMoveRename()) {
			mvInputStatus = isrp.isMovePathStatus() ? MRColDetailLog.MOVEINPUTSTATUS_SUCCESS
					: MRColDetailLog.MOVEINPUTSTATUS_FAIL;
			reInputStatus = isrp.isRenameRuleStatus() ? MRColDetailLog.RENAMEINPUTSTATUS_SUCCESS
					: MRColDetailLog.RENAMEINPUTSTATUS_FAIL;
			filelog.setRenameInputStatus(reInputStatus);
			filelog.setMoveInputStatus(mvInputStatus);
		}

		this.updateOutputDetailLog(isrp, filelog, delInputStatus, mvInputStatus, reInputStatus,
				null == lstOutput ? isrp.getLstOutputSourceRecord() : lstOutput);
	}

	/**
	 * 处理输入输出详细日志
	 * 
	 * @param isrp
	 * @param filelog
	 * @param delInputStatus
	 * @param mvInputStatus
	 * @param reInputStatus
	 * @param lstOutput
	 */
	private void updateOutputDetailLog(InputSourceRecordPO isrp, MRColDetailLog filelog, int delInputStatus,
			int mvInputStatus, int reInputStatus, List<OutputSourceRecordPO> lstOutput) {
		int mvOutputStatus = -1;
		int renameStatus = -1;
		for (OutputSourceRecordPO output : lstOutput) {
			filelog.setId(output.getColFileDetailId());
			filelog.setFileId(-1);
			filelog.setEndTime(StringUtil.dateToString(new Date(), StringUtil.DATE_FORMAT_TYPE1));

			// 输入重命名
			mvOutputStatus = -1;
			if (output.isMove()) {
				mvOutputStatus = output.isMovePathStatus() ? MRColDetailLog.MOVEOUTPUTSTATUS_SUCCESS
						: MRColDetailLog.MOVEOUTPUTSTATUS_FAIL;
			}
			filelog.setMoveOutputStatus(mvOutputStatus);

			// 输出重命名
			renameStatus = -1;
			if (output.isRename()) {
				renameStatus = output.isRenameRuleStatus() ? MRColDetailLog.OUTPUTRENAMESTATUS_SUCCESS
						: MRColDetailLog.OUTPUTRENAMESTATUS_FAIL;
			}
			filelog.setOutputRenameStatus(renameStatus);

			filelog.setOutputFileName(output.getNewName() == null ? StringUtil.getFileName(output.getPath()) : output
					.getNewName());

			if (mvInputStatus == MRColDetailLog.MOVEINPUTSTATUS_FAIL ||
					delInputStatus == MRColDetailLog.DELETEINPUTSTATUS_FAIL ||
					mvOutputStatus == MRColDetailLog.MOVEOUTPUTSTATUS_FAIL ||
					renameStatus == MRColDetailLog.OUTPUTRENAMESTATUS_FAIL ||
					output.getStatus() != OutputSourceRecordPO.STATUS_SUCCESS) {
				filelog.setStatus(MRColDetailLog.STATUS_FAIL);// 失败
			} else {
				filelog.setStatus(MRColDetailLog.STATUS_SUCCESS);// 成功
				if (this.FileSize > 0)// 更新输出文件真实大小
					filelog.setFileSize(this.FileSize);
			}
			MRLog.getInstance().updateColFileDetailLog(filelog);
		}
	}

	/**
	 * 添加文件到文件列表
	 * 
	 * @param isrp
	 */
	public void addFileToFileList(InputSourceRecordPO isrp) {
		MRFileListPO file = new MRFileListPO();
		int mvOutputStatus = -1;
		int renameStatus = -1;
		for (OutputSourceRecordPO output : isrp.getLstOutputSourceRecord()) {
			// 输出移动状态
			mvOutputStatus = -1;
			if (output.isMove()) {
				mvOutputStatus = output.isMovePathStatus() ? MRColDetailLog.MOVEOUTPUTSTATUS_SUCCESS
						: MRColDetailLog.MOVEOUTPUTSTATUS_FAIL;
			}

			// 输出重命名状态
			renameStatus = -1;
			if (output.isRename()) {
				renameStatus = output.isRenameRuleStatus() ? MRColDetailLog.OUTPUTRENAMESTATUS_SUCCESS
						: MRColDetailLog.OUTPUTRENAMESTATUS_FAIL;
			}

			// 获取文件路径
			String tmpfilePath = output.getPath();
			String fileNewPath = output.getNewPath();
			String filePath = (fileNewPath == null || fileNewPath.trim().length() <= 0) ? tmpfilePath : fileNewPath;

			String fileId = StringUtil.getFileOnlyKey(filePath, output.getDataSource().getId());
			MRFileListPO mfFilePo = MRLog.getInstance().getFilePoByFileId(fileId);

			// 成功才记录文件到文件列表
			if (!(mvOutputStatus == MRColDetailLog.MOVEOUTPUTSTATUS_FAIL || renameStatus == MRColDetailLog.OUTPUTRENAMESTATUS_FAIL)) {
				if (null == mfFilePo) {
					file.setDataSourceId(output.getDataSource().getId());
					file.setFileId(fileId);
					file.setFilePath(filePath);
					file.setFileSize(output.getSize());
					file.setRecordMonth(StringUtil.dateToString(new Date(), StringUtil.DATE_FORMAT_TYPE3));
					file.setRecordTime(StringUtil.dateToString(new Date(), StringUtil.DATE_FORMAT_TYPE1));
					MRLog.getInstance().addFileListLog(file);
				}
			}
		}
	}

	/**
	 * 获取输出流
	 * 
	 * @param outProtocol
	 * @param outFilePath
	 * @param outSystem
	 * @param os
	 * @param isDelOutputFile
	 * @param compress
	 * @param osrp
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws FTPException
	 * @throws Exception
	 */
	public OutputStream getOutPutStream(String outProtocol, String outFilePath, Object outSystem, OutputStream os,
			boolean isDelOutputFile, int compress, Configuration conf, OutputSourceRecordPO osrp)
			throws FileNotFoundException, IOException, FTPException, Exception {
		MRLog.systemOut(MRLog.get4Space() + "预先清除目标文件，是否清除：" + (isDelOutputFile ? "是" : "否"));
		if (Contant.PROTOCOL_LOCAL.equals(outProtocol)) {
			if (isDelOutputFile) {
				// 先删除之前的目标文件
				this.deleteLocalFileBefore(osrp);
				SourceUtil.deleteLocalFile(outFilePath);
			}
			File parent = new File(outFilePath.substring(0, outFilePath.lastIndexOf("/")));
			if (!parent.exists()) {
				parent.mkdirs();
			}
			os = new BufferedOutputStream(new FileOutputStream(new File(outFilePath)));
			os = HDFSUtils.getCompressOutputStream(os, compress);
		} else if (Contant.PROTOCOL_LOCALHDFS.equals(outProtocol) && outSystem instanceof FileSystem) {
			if (isDelOutputFile) {
				// 先删除之前的目标文件
				this.deleteHDFSFileBefore(osrp, (FileSystem) outSystem, osrp.getCompress());
				SourceUtil.deleteHDFSFile(outFilePath, (FileSystem) outSystem, compress);
			}
			System.out.println("FTP DOWN outFilePath=" + outFilePath + " compress=" + compress);
			os = HDFSUtils.getCompressOutputStream(outFilePath, outSystem, compress);
		} else if (Contant.PROTOCOL_REMOREHDFS.equals(outProtocol) && outSystem instanceof FileSystem) {
			if (isDelOutputFile) {
				// 先删除之前的目标文件
				this.deleteHDFSFileBefore(osrp, (FileSystem) outSystem, osrp.getCompress());
				SourceUtil.deleteHDFSFile(outFilePath, (FileSystem) outSystem, compress);
			}
			os = HDFSUtils.getCompressOutputStream(outFilePath, outSystem, compress);

		} else if (Contant.PROTOCOL_FTP.equals(outProtocol) && outSystem instanceof IFileTransfer) {
			if (isDelOutputFile) {
				// 先删除之前的目标文件
				this.deleteFTPFileBefore(osrp, osrp.getMovePathFile(), (IFileTransfer) outSystem);
				SourceUtil.deleteFTPFile(outFilePath, (IFileTransfer) outSystem, conf.get(MRConfiguration.CONNECT_MAX),
						conf.get(MRConfiguration.CONNECT_TIME));
			}
			((IFileTransfer) outSystem).mkdir(outFilePath.substring(0, outFilePath.lastIndexOf("/")),
					conf.get(MRConfiguration.CONNECT_MAX), conf.get(MRConfiguration.CONNECT_TIME));

			outFilePath = this.getOutputCompressFile(outFilePath, compress);
			os = ((IFileTransfer) outSystem).getOutputStream(outFilePath, conf.get(MRConfiguration.CONNECT_MAX),
					conf.get(MRConfiguration.CONNECT_TIME));
			os = HDFSUtils.getCompressOutputStream(os, compress);
		} else if (Contant.PROTOCOL_SFTP.equals(outProtocol) && outSystem instanceof IFileTransfer) {
			if (isDelOutputFile) {
				// 先删除之前的目标文件
				this.deleteFTPFileBefore(osrp, osrp.getMovePathFile(), (IFileTransfer) outSystem);
				SourceUtil.deleteFTPFile(outFilePath, (IFileTransfer) outSystem, conf.get(MRConfiguration.CONNECT_MAX),
						conf.get(MRConfiguration.CONNECT_TIME));
			}
			((IFileTransfer) outSystem).mkdir(outFilePath.substring(0, outFilePath.lastIndexOf("/")),
					conf.get(MRConfiguration.CONNECT_MAX), conf.get(MRConfiguration.CONNECT_TIME));
			outFilePath = this.getOutputCompressFile(outFilePath, compress);
			os = ((IFileTransfer) outSystem).getOutputStream(outFilePath, conf.get(MRConfiguration.CONNECT_MAX),
					conf.get(MRConfiguration.CONNECT_TIME));
			os = HDFSUtils.getCompressOutputStream(os, compress);
		}
		return os;
	}

	public InputStream getInputStream(Object inputSourceSystem, String inputSourceProtocol, String inputPath,
			Configuration conf) throws FileNotFoundException, IOException, Exception {
		// 获取输入流
		if (Contant.PROTOCOL_LOCAL.equals(inputSourceProtocol)) {
			return new BufferedInputStream(new FileInputStream(new File(inputPath)));
		} else if (Contant.PROTOCOL_LOCALHDFS.equals(inputSourceProtocol) && inputSourceSystem instanceof FileSystem) {
			return ((FileSystem) inputSourceSystem).open(new Path(inputPath), 8192);
		} else if (Contant.PROTOCOL_REMOREHDFS.equals(inputSourceProtocol) && inputSourceSystem instanceof FileSystem) {
			return ((FileSystem) inputSourceSystem).open(new Path(inputPath), 8192);
		} else if (Contant.PROTOCOL_FTP.equals(inputSourceProtocol) && inputSourceSystem instanceof IFileTransfer) {
			return ((IFileTransfer) inputSourceSystem).getInputStream(inputPath, conf.get(MRConfiguration.CONNECT_MAX),
					conf.get(MRConfiguration.CONNECT_TIME));
		} else if (Contant.PROTOCOL_SFTP.equals(inputSourceProtocol) && inputSourceSystem instanceof IFileTransfer) {
			return ((IFileTransfer) inputSourceSystem).getInputStream(inputPath, conf.get(MRConfiguration.CONNECT_MAX),
					conf.get(MRConfiguration.CONNECT_TIME));
		}

		return null;
	}

	/**
	 * 处理传输成功后，输出文件的处理
	 * 
	 * @param conf
	 * @param values
	 */
	public void handleOutputFile(Configuration conf, OutputSourceRecordPO osrp) {
		// 判断文件是否是文件.
		FTPDataSourcePO ftpds = osrp.getDataSource();
		String protocol = ftpds.getProtocol();
		String path = osrp.getPath();
		String newName = osrp.getNewName();
		String newPath = osrp.getRenameNewPath();
		Object objSourceSystem = SourceUtil.initServer(osrp.getDataSource());
		osrp.setOutputSourceSystem(objSourceSystem);
		boolean rs = true;
		MRLog.systemOut(MRLog.get4Space() + "开始处理, 协议类型:" + protocol);
		if (Contant.PROTOCOL_LOCAL.equals(protocol)) {
			rs = this.renameLocalFile(osrp, path, newPath);
			MRLog.systemOut(MRLog.get4Space() + "重命名结果：" + (rs ? "成功" : "失败"));
			if (rs) {
				this.moveLocalFile(osrp);
			}
		} else if (null != objSourceSystem && Contant.PROTOCOL_LOCALHDFS.equals(protocol) &&
				objSourceSystem instanceof FileSystem) {
			FileSystem fileSystem = (FileSystem) objSourceSystem;
			rs = this.renameHDFSFile(osrp, path, newPath, fileSystem);

			MRLog.systemOut(MRLog.get4Space() + "重命名结果：" + (rs ? "成功" : "失败"));
			if (rs) {
				this.moveHDFSFile(osrp, fileSystem, osrp.getCompress());
			}
		} else if (null != objSourceSystem && Contant.PROTOCOL_REMOREHDFS.equals(protocol) &&
				objSourceSystem instanceof FileSystem) {
			FileSystem fileSystem = (FileSystem) objSourceSystem;
			rs = this.renameHDFSFile(osrp, path, newPath, fileSystem);

			MRLog.systemOut(MRLog.get4Space() + "重命名结果：" + (rs ? "成功" : "失败"));
			if (rs) {
				this.moveHDFSFile(osrp, fileSystem, osrp.getCompress());
			}
		} else if (null != objSourceSystem && Contant.PROTOCOL_FTP.equals(protocol) &&
				objSourceSystem instanceof IFileTransfer) {
			IFileTransfer fileTransfer = (IFileTransfer) objSourceSystem;
			rs = this.renameFTPFile(osrp, path, newName, fileTransfer, protocol);

			MRLog.systemOut(MRLog.get4Space() + "重命名结果：" + (rs ? "成功" : "失败"));
			if (rs) {
				this.moveFTPFile(osrp, fileTransfer, protocol, osrp.getCompress());
			}
		} else if (null != objSourceSystem && Contant.PROTOCOL_SFTP.equals(protocol) &&
				objSourceSystem instanceof IFileTransfer) {
			IFileTransfer fileTransfer = (IFileTransfer) objSourceSystem;
			rs = this.renameFTPFile(osrp, path, newName, fileTransfer, protocol);

			MRLog.systemOut(MRLog.get4Space() + "重命名结果：" + (rs ? "成功" : "失败"));
			if (rs) {
				this.moveFTPFile(osrp, fileTransfer, protocol, osrp.getCompress());
			}
		}
	}

	/**
	 * 处理传输成功后，输入文件的处理
	 * 
	 * @param conf
	 * @param values
	 */
	public void handleInputFile(Configuration conf, InputSourceRecordPO isrp) {
		// 判断文件是否是文件.
		FTPDataSourcePO ftpds = isrp.getDataSource();
		String protocol = ftpds.getProtocol();
		String path = isrp.getPath();
		String newPath = isrp.getRenameNewPath();
		String newName = isrp.getNewName();
		Object objSourceSystem = SourceUtil.initServer(isrp.getDataSource());
		isrp.setInputSourceSystem(objSourceSystem);
		MRLog.systemOut(MRLog.get4Space() + "开始处理, 协议类型:" + protocol);
		MRLog.systemOut(MRLog.get4Space() + "处理的标示状态=> 删除:" + (isrp.isDelete() ? "是" : "否") + ", 重命名:" +
				(isrp.isRename() ? "是" : "否") + ", 移动:" + (isrp.isMove() ? "是" : "否") + ", 重命名并移动:" +
				(isrp.isMoveRename() ? "是" : "否"));
		if (Contant.PROTOCOL_LOCAL.equals(protocol)) {
			if (isrp.isDelete()) {
				this.delLocalFile(isrp, path);
			}
			if (isrp.isRename()) {
				boolean rs = this.renameLocalFile(isrp, path, newPath);
				MRLog.systemOut(MRLog.get4Space() + "重命名结果：" + (rs ? "成功" : "失败"));
			}
			if (isrp.isMove()) {
				this.moveLocalFile(isrp);
			}
			if (isrp.isMoveRename()) {
				boolean rs = this.renameLocalFile(isrp, path, newPath);
				MRLog.systemOut(MRLog.get4Space() + "重命名结果：" + (rs ? "成功" : "失败"));
				this.moveLocalFile(isrp);
			}
		} else if (null != objSourceSystem && Contant.PROTOCOL_LOCALHDFS.equals(protocol) &&
				objSourceSystem instanceof FileSystem) {
			FileSystem fileSystem = (FileSystem) objSourceSystem;
			if (isrp.isDelete()) {
				this.delHDFSFile(isrp, path, fileSystem);
			}
			if (isrp.isRename()) {
				boolean rs = this.renameHDFSFile(isrp, path, newPath, fileSystem);
				MRLog.systemOut(MRLog.get4Space() + "重命名结果：" + (rs ? "成功" : "失败"));
			}
			if (isrp.isMove()) {
				this.moveHDFSFile(isrp, fileSystem, 0);
			}
			if (isrp.isMoveRename()) {
				boolean rs = this.renameHDFSFile(isrp, path, newPath, fileSystem);
				MRLog.systemOut(MRLog.get4Space() + "重命名结果：" + (rs ? "成功" : "失败"));
				this.moveHDFSFile(isrp, fileSystem, 0);
			}
		} else if (null != objSourceSystem && Contant.PROTOCOL_REMOREHDFS.equals(protocol) &&
				objSourceSystem instanceof FileSystem) {
			FileSystem fileSystem = (FileSystem) objSourceSystem;
			if (isrp.isDelete()) {
				this.delHDFSFile(isrp, path, fileSystem);
			}
			if (isrp.isRename()) {
				boolean rs = this.renameHDFSFile(isrp, path, newPath, fileSystem);
				MRLog.systemOut(MRLog.get4Space() + "重命名结果：" + (rs ? "成功" : "失败"));
			}
			if (isrp.isMove()) {
				this.moveHDFSFile(isrp, fileSystem, 0);
			}
			if (isrp.isMoveRename()) {
				boolean rs = this.renameHDFSFile(isrp, path, newPath, fileSystem);
				MRLog.systemOut(MRLog.get4Space() + "重命名结果：" + (rs ? "成功" : "失败"));
				this.moveHDFSFile(isrp, fileSystem, 0);
			}
		} else if (null != objSourceSystem && Contant.PROTOCOL_FTP.equals(protocol) &&
				objSourceSystem instanceof IFileTransfer) {
			IFileTransfer fileTransfer = (IFileTransfer) objSourceSystem;
			if (isrp.isDelete()) {
				this.delFTPFile(isrp, path, fileTransfer);
			}

			if (isrp.isRename()) {
				boolean rs = this.renameFTPFile(isrp, path, newName, fileTransfer, protocol);
				MRLog.systemOut(MRLog.get4Space() + "重命名结果：" + (rs ? "成功" : "失败"));
			}

			if (isrp.isMove()) {
				this.moveFTPFile(isrp, fileTransfer, protocol, 0);
			}

			if (isrp.isMoveRename()) {
				boolean rs = this.renameFTPFile(isrp, path, newName, fileTransfer, protocol);
				if (rs) {
					this.moveFTPFile(isrp, fileTransfer, protocol, 0);
				}
			}
		} else if (null != objSourceSystem && Contant.PROTOCOL_SFTP.equals(protocol) &&
				objSourceSystem instanceof IFileTransfer) {
			IFileTransfer fileTransfer = (IFileTransfer) objSourceSystem;
			if (isrp.isDelete()) {
				this.delFTPFile(isrp, path, fileTransfer);
			}

			if (isrp.isRename()) {
				boolean rs = this.renameFTPFile(isrp, path, newName, fileTransfer, protocol);
				MRLog.systemOut(MRLog.get4Space() + "重命名结果：" + (rs ? "成功" : "失败"));
			}

			if (isrp.isMove()) {
				this.moveFTPFile(isrp, fileTransfer, protocol, 0);
			}

			if (isrp.isMoveRename()) {
				boolean rs = this.renameFTPFile(isrp, path, newName, fileTransfer, protocol);
				MRLog.systemOut(MRLog.get4Space() + "重命名结果：" + (rs ? "成功" : "失败"));
				if (rs) {
					this.moveFTPFile(isrp, fileTransfer, protocol, 0);
				}
			}
		}
	}

	public void moveFTPFile(SourceRecordPO srp, IFileTransfer fileTransfer, String protocol, int compress) {
		if (srp.isMovePathStatus()) {
			MRLog.systemOut(MRLog.get4Space() + "是否需要移动:否, 因为已经移动成功");
			return;
		}

		String path = srp.getPath();
		String movePath = srp.getMovePath();
		String newPath = srp.getRenameNewPath();
		String movePathFile = srp.getMovePathFile();

		if (null == movePath || movePath.trim().length() <= 0) {
			srp.setMovePathStatus(false);
			MRLog.systemOut(MRLog.get4Space() + "是否需要移动:否");
			return;
		}

		MRLog.systemOut(MRLog.get4Space() + "是否需要移动:是");
		try {
			if (!fileTransfer.exist(movePath)) {
				fileTransfer.mkdir(movePath, "0", "0");
			}

		} catch (Exception e) {
			srp.setMovePathStatus(false);
			MRLog.systemOut(MRLog.get4Space() + "移动结果:失败(创建目录失败)");
			e.printStackTrace();
		}

		String filePath = null != newPath ? newPath : path;
		MRLog.systemOut(MRLog.get6Space() + "移动原路径:" + filePath + ", 目标路径:" + movePathFile);
		if (filePath.equals(movePathFile)) {
			srp.setMovePathStatus(true);
			MRLog.systemOut(MRLog.get4Space() + "移动结果:成功");
			return;
		}
		try {
			if (fileTransfer.exist(filePath) && fileTransfer.isFile(filePath)) {
				fileTransfer.moveFile(filePath, movePathFile);
				srp.setMovePathStatus(true);
				MRLog.systemOut(MRLog.get4Space() + "移动结果:成功");
			}
		} catch (Exception e) {
			e.printStackTrace();
			srp.setMovePathStatus(false);
			MRLog.systemOut(MRLog.get4Space() + "移动结果:失败");
		}
	}

	public boolean renameFTPFile(SourceRecordPO srp, String filepath, String newName, IFileTransfer fileTransfer,
			String protocol) {
		String renameNewPath = srp.getRenameNewPath();
		if (srp.isRenameRuleStatus()) {
			MRLog.systemOut(MRLog.get4Space() + "是否需要重命名:否, 因为已经重命名成功");
			return true;
		}

		if (null == newName) {
			MRLog.systemOut(MRLog.get4Space() + "是否需要重命名:否");
			return true;
		}

		MRLog.systemOut(MRLog.get4Space() + "是否需要重命名:是");
		MRLog.systemOut(MRLog.get6Space() + "重命名源文件路径：" + filepath + "; 目标路径:" + renameNewPath);
		if (renameNewPath.equals(filepath)) {
			srp.setRenameRuleStatus(true);
			return true;
		}

		try {
			if (fileTransfer.exist(filepath) && fileTransfer.isFile(filepath)) {
				fileTransfer.renameTo(filepath, newName);
				srp.setRenameRuleStatus(true);
			} else {
				srp.setRenameRuleStatus(false);
			}
		} catch (Exception e) {
			srp.setRenameRuleStatus(false);
			e.printStackTrace();
		}
		return srp.isRenameRuleStatus();
	}

	public void delFTPFile(InputSourceRecordPO srp, String filepath, IFileTransfer fileTransfer) {
		MRLog.systemOut(MRLog.get6Space() + "删除路径：" + filepath);
		try {
			if (fileTransfer.exist(filepath) && fileTransfer.isFile(filepath)) {
				fileTransfer.delete(filepath, "0", "0");
				srp.setDeleteStatus(true);
				MRLog.systemOut(MRLog.get4Space() + "删除结果：成功");
			} else {
				srp.setDeleteStatus(false);
				MRLog.systemOut(MRLog.get4Space() + "删除结果：失败");
			}
		} catch (Exception e) {
			srp.setDeleteStatus(false);
			e.printStackTrace();
			MRLog.systemOut(MRLog.get4Space() + "删除结果：失败");
		}
	}

	public void moveHDFSFile(SourceRecordPO srp, FileSystem fileSystem, int compress) {
		String path = srp.getPath();
		String movePath = srp.getMovePath();
		String newPath = srp.getRenameNewPath();
		String movePathFile = srp.getMovePathFile();
		if (null == movePath || movePath.trim().length() <= 0) {
			MRLog.systemOut(MRLog.get4Space() + "是否需要移动:否");
			return;
		}

		MRLog.systemOut(MRLog.get4Space() + "是否需要移动:是");
		Path mvFile = new Path(movePath);
		try {
			if (!fileSystem.exists(mvFile)) {
				fileSystem.mkdirs(mvFile);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		String initPath = null != newPath ? newPath : path;
		MRLog.systemOut(MRLog.get6Space() + "移动原路径:" + initPath + ", 目标路径:" + movePathFile);
		Path file = new Path(initPath);
		if (file.toUri().toString().equals(movePathFile)) {
			srp.setMovePathStatus(true);
			MRLog.systemOut(MRLog.get4Space() + "移动结果:成功");
			return;
		}

		try {
			if (fileSystem.exists(file) && fileSystem.isFile(file)) {
				file.getFileSystem(fileSystem.getConf()).rename(file, new Path(movePathFile));
				srp.setMovePathStatus(true);
				srp.setNewPath(movePathFile);
				MRLog.systemOut(MRLog.get4Space() + "移动结果:成功");
				if (fileSystem.exists(file) && fileSystem.isFile(file)) {
					fileSystem.delete(file, false);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			srp.setMovePathStatus(false);
			MRLog.systemOut(MRLog.get4Space() + "移动结果:失败");
		}
	}

	public boolean renameHDFSFile(SourceRecordPO srp, String filepath, String newPath, FileSystem fileSystem) {
		if (null == newPath) {
			MRLog.systemOut(MRLog.get4Space() + "是否需要重命名:否");
			return true;
		}

		MRLog.systemOut(MRLog.get4Space() + "是否需要重命名:是");
		MRLog.systemOut(MRLog.get6Space() + "重命名源文件路径：" + filepath + "; 目标路径:" + newPath);
		if (newPath.equals(filepath)) {
			srp.setRenameRuleStatus(true);
			return true;
		}

		Path file = new Path(filepath);
		try {
			if (fileSystem.exists(file) && fileSystem.isFile(file)) {
				boolean rs = file.getFileSystem(fileSystem.getConf()).rename(file, new Path(newPath));
				if (rs) {
					srp.setRenameRuleStatus(true);
					srp.setNewPath(newPath);
					if (fileSystem.exists(file) && fileSystem.isFile(file)) {
						fileSystem.delete(file, false);
					}
				} else {
					srp.setRenameRuleStatus(false);
				}
			} else {
				srp.setRenameRuleStatus(false);
			}
		} catch (IOException e) {
			e.printStackTrace();
			srp.setRenameRuleStatus(false);
		}

		return srp.isRenameRuleStatus();
	}

	public void delHDFSFile(InputSourceRecordPO srp, String filepath, FileSystem fileSystem) {
		MRLog.systemOut(MRLog.get6Space() + "删除路径：" + filepath);
		Path file = new Path(filepath);
		try {
			if (fileSystem.exists(file) && fileSystem.isFile(file)) {
				fileSystem.deleteOnExit(file);
				srp.setDeleteStatus(true);
				MRLog.systemOut(MRLog.get4Space() + "删除结果：成功");
			} else {
				srp.setDeleteStatus(false);
				MRLog.systemOut(MRLog.get4Space() + "删除结果：失败");
			}
		} catch (IOException e) {
			srp.setDeleteStatus(false);
			e.printStackTrace();
			MRLog.systemOut(MRLog.get4Space() + "删除结果：失败");
		}
	}

	public void moveLocalFile(SourceRecordPO srp) {
		String path = srp.getPath();
		String movePath = srp.getMovePath();
		String newPath = srp.getRenameNewPath();
		String movePathFile = srp.getMovePathFile();
		if (null == movePath || movePath.trim().length() <= 0) {
			MRLog.systemOut(MRLog.get4Space() + "是否需要移动:否");
			return;
		}

		MRLog.systemOut(MRLog.get4Space() + "是否需要移动:是");
		File mvFile = new File(movePath);
		if (!mvFile.exists()) {
			mvFile.mkdirs();
		}

		if (!mvFile.isDirectory()) {
			return;
		}

		File file = null != newPath ? new File(newPath) : new File(path);
		MRLog.systemOut(MRLog.get6Space() + "移动原路径:" + file.getPath() + ", 目标路径:" + movePathFile);
		if (file.getPath().equals(movePathFile)) {
			srp.setMovePathStatus(true);
			MRLog.systemOut(MRLog.get4Space() + "移动结果:成功");
			return;
		}

		if (file.exists() && file.isFile()) {
			String cmd = "mv " + file.getAbsolutePath() + " " + (new File(movePathFile).getParent());
			try {
				Runtime.getRuntime().exec(cmd);
				srp.setMovePathStatus(true);
			} catch (IOException e) {
				e.printStackTrace();
				srp.setMovePathStatus(false);
			}

			MRLog.systemOut(MRLog.get4Space() + "移动结果:" + (srp.isMovePathStatus() ? "成功" : "失败"));
		}
	}

	public boolean renameLocalFile(SourceRecordPO srp, String filepath, String newPath) {
		MRLog.systemOut(MRLog.get4Space() + "是否需要重命名:" + (srp.isRenameRuleStatus() ? "否" : "是"));
		if (null != newPath) {
			if (!srp.isRenameRuleStatus()) {
				if (newPath.equals(filepath)) {
					srp.setRenameRuleStatus(true);
					return true;
				}
				File file = new File(filepath);
				if (file.exists() && file.isFile()) {
					MRLog.systemOut(MRLog.get6Space() + "重命名源文件路径：" + file.getAbsolutePath() + "; 目标路径:" + newPath);
					boolean rs = file.renameTo(new File(newPath));
					if (rs) {
						srp.setRenameRuleStatus(true);
					} else {
						srp.setRenameRuleStatus(false);
					}
				} else {
					srp.setRenameRuleStatus(false);
				}
			}
			return srp.isRenameRuleStatus();
		}

		return true;
	}

	public void delLocalFile(InputSourceRecordPO srp, String filepath) {
		MRLog.systemOut(MRLog.get6Space() + "删除路径：" + filepath);
		File file = new File(filepath);
		if (file.exists() && file.isFile()) {
			file.delete();
			srp.setDeleteStatus(true);
			MRLog.systemOut(MRLog.get4Space() + "删除结果：成功");
		} else {
			srp.setDeleteStatus(false);
			MRLog.systemOut(MRLog.get4Space() + "删除结果：失败");
		}
	}

	/**
	 * 获取封装的输入压缩流
	 * 
	 * @param stream
	 * @return
	 */
	public InputStream getCompressInputStream(InputStream stream) {
		try {
			if (this.compressType == CompressType.GZ) {
				return new GZIPInputStream(stream);
			} else if (this.compressType == CompressType.TAR) {
				return new TarInputStream(new BufferedInputStream(stream));
			} else if (this.compressType == CompressType.TARGZ) {
				return new TarInputStream(new BufferedInputStream(new GZIPInputStream(stream)));
			}
		} catch (IOException e) {
			MRLog.errorException(LOG, e);
		}

		return stream;
	}

	/**
	 * 获取封装的输出压缩流
	 * 
	 * @param stream
	 * @return
	 */
	public OutputStream getCompressOutputStream(OutputStream stream) {
		try {
			if (this.compressType == CompressType.GZ || this.compressType == CompressType.TARGZ) {
				return new GZIPOutputStream(stream);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return stream;
	}

	private String getOutputCompressFile(String outFilePath, int compress) throws IOException {
		if (compress == 1) {
			if (!outFilePath.endsWith(".gz")) {
				outFilePath += ".gz";
			}
		}
		return outFilePath;
	}

	/**
	 * 是删除文件
	 */
	private void deleteLocalFileBefore(OutputSourceRecordPO osrp) {
		String movePathFile = osrp.getMovePathFile();
		File file = new File(movePathFile);
		if (file.exists() && file.isFile()) {
			file.delete();
			MRLog.systemOut(MRLog.get6Space() + "成功清除移动目录文件:" + movePathFile);
		}
	}

	/**
	 * 是删除文件
	 */
	public void deleteHDFSFileBefore(SourceRecordPO srp, FileSystem fileSystem, int compress) {
		String movePath = srp.getMovePath();
		String movePathFile = srp.getMovePathFile();
		if (null == movePath || movePath.trim().length() <= 0) {
			return;
		}

		Path file = new Path(movePathFile);
		try {
			if (fileSystem.exists(file) && fileSystem.isFile(file)) {
				fileSystem.deleteOnExit(file);
				MRLog.systemOut(MRLog.get6Space() + "成功清除移动目录文件:" + movePathFile);
			}
		} catch (IOException e) {
		}
	}

	/**
	 * 是删除文件
	 */
	public void deleteFTPFileBefore(SourceRecordPO srp, String filepath, IFileTransfer fileTransfer) {
		try {
			if (fileTransfer.exist(filepath) && fileTransfer.isFile(filepath)) {
				fileTransfer.delete(filepath, "0", "0");
				MRLog.systemOut(MRLog.get6Space() + "成功清除移动目录文件:" + filepath);
			}
		} catch (Exception e) {
		}
	}

	public class InputIOException extends IOException {

		/**
		 * 
		 */
		private static final long serialVersionUID = 4709915007070792293L;

	}

	public class OutputIOException extends IOException {

		/**
		 * 
		 */
		private static final long serialVersionUID = -6117610717610273640L;

	}

	public IRemotePlugin getRemotePlugin() {
		return remotePlugin;
	}

	public void setRemotePlugin(IRemotePlugin remotePlugin) {
		this.remotePlugin = remotePlugin;
	}

	public enum CompressType {
		TAR, TARGZ, GZ, NONE
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		this.fileEncodeing = this.conf.get(MRConfiguration.FILE_CONTENT_ENCODING,
				MRConfiguration.FILE_CONTENT_ENCODING_DEFAULT).toLowerCase();
		if (this.fileEncodeing.equals("")) {
			this.fileEncodeing = "utf-8";
		}
		this.fileEncodeingIsUTF8 = this.fileEncodeing.equals(MRConfiguration.FILE_CONTENT_ENCODING_DEFAULT);
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}
}