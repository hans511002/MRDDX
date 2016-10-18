package com.ery.hadoop.mrddx.hFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.zookeeper.KeeperException;

import com.ery.hadoop.mrddx.IHandleFormat;
import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.file.FileConfiguration;
import com.ery.hadoop.mrddx.file.FileSplits;
import com.ery.hadoop.mrddx.file.RegexPathFilter;
import com.ery.hadoop.mrddx.log.MRJobMapDataLog;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.mode.MRFileListPO;
import com.ery.hadoop.mrddx.util.StringUtil;
import com.ery.hadoop.mrddx.zk.MRFileZookeeper;

public class TextInputFormat extends org.apache.hadoop.mapreduce.lib.input.TextInputFormat implements IHandleFormat {

	// 日志对象
	public static final Log LOG = LogFactory.getLog(TextInputFormat.class);

	// 输入文件的数量
	static final String NUM_INPUT_FILES = "mapreduce.input.num.files";
	private static final MRLog MRLOG = MRLog.getInstance();

	/**
	 * 构造方法
	 */
	public TextInputFormat() {
	}

	private static final PathFilter hiddenFileFilter = new PathFilter() {
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".");
		}
	};

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
		// 日志
		long logId = context.getConfiguration().getLong(MRConfiguration.INTERNAL_JOB_LOG_ID, -1);
		MRLog.info(LOG, logId, "Get text recordReader");
		return new LineRecordReader();
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		List<InputSplit> splits = super.getSplits(job);
		long totalSize = 0; // compute total size
		List<InputSplit> lstFileSplit = new ArrayList<InputSplit>();
		for (InputSplit split : splits) { // check we have valid files
			if (split instanceof FileSplits) {
				FileSplits fileSplit = (FileSplits) split;
				FileSplits newfileSplit = new FileSplits(fileSplit.getPath(), fileSplit.getStart(),
						fileSplit.getLength(), fileSplit.getLocations());
				lstFileSplit.add(newfileSplit);
				totalSize += fileSplit.getLength();
				MRLog.info(LOG, "File=>>" + fileSplit.getPath().toUri().toString());
			}
		}

		// 日志 文件大小
		FileConfiguration fileconf = new FileConfiguration(job.getConfiguration());
		MRLOG.updateFileSizeJobRun(fileconf.getJobLogId(), totalSize);

		MRLog.info(LOG, "Total # of splits: " + lstFileSplit.size());
		this.checkFileSplit(fileconf, lstFileSplit);
		this.unlockJOBIDZKNode(new MRConfiguration(job.getConfiguration()));
		MRLog.info(LOG, "Effective Total # of splits: " + lstFileSplit.size());
		return lstFileSplit;
	}

	/**
	 * 根据jobid+文件id查询最大的日志id； 如果状态为初始化、在运行、成功处理，则不处理；
	 * 如果状态为失败，或者记录不存在，则初始化文件处理信息，并且如果失败的需要删除之前的记录（在map中解析文件之前删除）
	 * 如果有强制处理标示符，则不需要根据jobid+文件id查询最大的日志id，直接新增文件处理日志
	 * 
	 * @param fileconf
	 * @param splits
	 * @throws IOException
	 */
	private void checkFileSplit(FileConfiguration fileconf, List<InputSplit> splits) throws IOException {
		// 判断是否有强制处理标示
		int jobLogType = fileconf.getInputFileJobLogType();
		List<MRJobMapDataLog> dataLogFail = null;
		switch (jobLogType) {
		case 1:// 情况一：强制处理
			for (InputSplit split : splits) {
				FileSplits fileSplit = (FileSplits) split;
				// 根据jobid+文件id查询最大的日志id
				Path path = fileSplit.getPath();
				String filePath = path.toUri().getPath().toString();
				String fileId = StringUtil.getFileOnlyKey(filePath, fileconf.getInputDataSourceId());
				// 检查文件是否存在，不存在加入文件表中
				this.addFileToFileList(fileSplit, fileconf, fileId);
				this.initMRDataLog(fileSplit, fileconf, filePath, fileId, fileconf.getInputDataSourceId());
			}
			break;
		case 2:// 情况二：处理某次的文件处理(只处理失败)
		case 3:// 情况三：某个时间段的文件处理(只处理失败)
		case 4:// 情况四：执行所有的文件处理(只处理失败)
			dataLogFail = MRLOG.getMRJobMapDataLog(fileconf, jobLogType);// 查询处理日志信息.
			List<FileSplits> lstFileSplit = new ArrayList<FileSplits>();
			for (MRJobMapDataLog mrJobMapDataLog : dataLogFail) {// 处理失败的情况
				switch (mrJobMapDataLog.getStatus()) {
				case MRJobMapDataLog.STATUS_INIT:// 初始化
				case MRJobMapDataLog.STATUS_DOING:// 处理中
				case MRJobMapDataLog.STATUS_SUCCESS:// 成功
					continue;
				case MRJobMapDataLog.STATUS_FAILD:// 失败
					FileSplits fileSplit = this.getFaildSplits(fileconf, mrJobMapDataLog);
					if (null == fileSplit) {
						MRLOG.updateMRJobMapDataLogStatus(mrJobMapDataLog.getId(), MRJobMapDataLog.STATUS_DEL);
						continue;
					} else {
						MRLOG.updateMRJobMapDataLogStatus(mrJobMapDataLog.getId(), MRJobMapDataLog.STATUS_INIT);
					}
					fileSplit.setDeleteBeforeData(true);
					fileSplit.setFileLogId(mrJobMapDataLog.getId());
					fileSplit.setFileId(mrJobMapDataLog.getFileid());
					lstFileSplit.add(fileSplit);
					MRLog.info(LOG, "Effective File=>>" + fileSplit.getPath().toUri().toString());
					break;
				default:
					break;
				}
			}
			splits.clear();
			splits.addAll(lstFileSplit);
			break;
		default:// 初始化的第一次处理(全部处理),或者是处理当前最新一次的(只处理失败))
			List<FileSplits> lstRemoveFileSplit = new ArrayList<FileSplits>();
			for (InputSplit split : splits) {
				FileSplits fileSplit = (FileSplits) split;
				// 根据jobid+文件id查询最大的日志id
				Path path = fileSplit.getPath();
				String filePath = path.toUri().getPath().toString();
				String fileId = StringUtil.getFileOnlyKey(filePath, fileconf.getInputDataSourceId());
				MRLog.info(LOG, "jobId=" + fileconf.getSysJobId() + " , fileId=" + fileId);
				MRJobMapDataLog dataLog = MRLOG.getMRJobMapDataLog(fileconf.getSysJobId(), fileId);// 查询处理日志信息.
				// 检查文件是否存在，不存在加入文件表中
				this.addFileToFileList(fileSplit, fileconf, fileId);
				MRLog.info(LOG, "dataLog=" + dataLog);
				if (null == dataLog) {
					this.initMRDataLog(fileSplit, fileconf, filePath, fileId, fileconf.getInputDataSourceId());
					continue;
				}
				MRLog.info(LOG, "dataLog.getStatus()=" + dataLog.getStatus());
				switch (dataLog.getStatus()) {
				case MRJobMapDataLog.STATUS_INIT:// 初始化
				case MRJobMapDataLog.STATUS_DOING:// 处理中
				case MRJobMapDataLog.STATUS_SUCCESS:// 成功
					lstRemoveFileSplit.add(fileSplit);
					continue;
				case MRJobMapDataLog.STATUS_FAILD:// 失败
					MRLOG.updateMRJobMapDataLogStatus(dataLog.getId(), MRJobMapDataLog.STATUS_INIT);
					fileSplit.setDeleteBeforeData(true);
					fileSplit.setFileLogId(dataLog.getId());
					fileSplit.setFileId(fileId);
					break;
				default:
					break;
				}
				break;
			}

			// 移除不需处理的文件列表
			for (FileSplits fileSplit : lstRemoveFileSplit) {
				splits.remove(fileSplit);
			}
		}
	}

	private FileSplits getFaildSplits(FileConfiguration fileconf, MRJobMapDataLog mrJobMapDataLog) throws IOException {
		Configuration conf = new Configuration();
		// String hdfsAddress = mrJobMapDataLog.getHdfsAddress();
		// if (null == hdfsAddress || hdfsAddress.trim().length() <= 0) {
		// return null;
		// }
		// conf.set(MRConfiguration.FS_DEFAULT_NAME, hdfsAddress);

		String path = mrJobMapDataLog.getFilePath();
		Path hdfsPath = new Path(path);
		FileSystem fs = hdfsPath.getFileSystem(conf);
		FileStatus file = fs.getFileStatus(hdfsPath);
		if (fs.exists(hdfsPath) && fs.isFile(hdfsPath) && !file.isDir()) {
			long length = file.getLen();
			BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
			if (length != 0) {// 按文件数量拆分
				return new FileSplits(hdfsPath, 0l, length, blkLocations[0].getHosts());
			} else {
				// Create empty hosts array for zero length files
				return new FileSplits(hdfsPath, 0l, length, new String[0]);
			}
		}
		return null;
	}

	/**
	 * 解锁处理文件的JOBID节点
	 * 
	 * @param mrconf
	 */
	private void unlockJOBIDZKNode(MRConfiguration mrconf) {
		MRFileZookeeper mrFileZK = new MRFileZookeeper(mrconf.getConf());
		try {
			mrFileZK.init();
			mrFileZK.unlock();
			mrFileZK.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 初始化日志信息
	 * 
	 * @param inputSplit
	 * @param mrconf
	 * @param fileId
	 * @param dataSourceId
	 */
	private void initMRDataLog(FileSplits fileSplit, FileConfiguration fileconf, String filePath, String fileId,
			int dataSourceId) {
		MRJobMapDataLog dataLog = new MRJobMapDataLog();
		dataLog.setDataType(MRJobMapDataLog.DATATYPE_TXT);
		dataLog.setFilePath(filePath);
		dataLog.setFileSize(fileSplit.getLength());
		dataLog.setJobid(String.valueOf(fileconf.getSysJobId()));
		dataLog.setStartTime(StringUtil.dateToString(new Date(), StringUtil.DATE_FORMAT_TYPE1));
		dataLog.setStatus(MRJobMapDataLog.STATUS_INIT);
		dataLog.setFileid(fileId);
		dataLog.setJobLogId(fileconf.getJobLogId());
		dataLog.setDataSourceId(dataSourceId);
		MRLOG.addMRJobMapDataLog(dataLog);
		fileSplit.setFileLogId(dataLog.getId());// 设置初始日志id
		fileSplit.setFileId(fileId);
	}

	@Override
	protected List<FileStatus> listStatus(JobContext job) throws IOException {
		Path[] dirs = getInputPaths(job);
		if (dirs.length == 0) {
			throw new IOException("No input paths specified in job");
		}

		// get tokens for all the required FileSystems..
		TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs, job.getConfiguration());

		List<FileStatus> result = new ArrayList<FileStatus>();
		List<IOException> errors = new ArrayList<IOException>();

		// creates a MultiPathFilter with the hiddenFileFilter and the
		// user provided one (if any).
		List<PathFilter> filters = new ArrayList<PathFilter>();
		filters.add(hiddenFileFilter);
		PathFilter jobFilter = getInputPathFilter(job);
		if (jobFilter != null) {
			filters.add(jobFilter);
		}
		PathFilter inputFilter = new MultiPathFilter(filters);
		for (Path p : dirs) {
			FileSystem fs = p.getFileSystem(job.getConfiguration());
			FileStatus[] matches = fs.globStatus(p, inputFilter);
			if (matches == null) {
				errors.add(new IOException("Input path does not exist: " + p));
			} else if (matches.length == 0) {
				errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
			} else {
				for (FileStatus globStat : matches) {
					if (globStat.isDir()) {
						for (FileStatus stat : fs.listStatus(globStat.getPath(), inputFilter)) {
							result.add(stat);
						}
					} else {
						result.add(globStat);
					}
				}
			}
		}

		if (!errors.isEmpty()) {
			throw new InvalidInputException(errors);
		}

		// 获取子目录下的文件并添加到列表中
		List<FileStatus> lsRemoveFoldFile = new ArrayList<FileStatus>();
		for (int i = 0; i < result.size(); i++) {
			FileStatus file = result.get(i);
			if (file.isDir()) {
				lsRemoveFoldFile.add(file);
				List<FileStatus> subs = getSubFileStatus(file, job, inputFilter);
				if (subs != null) {
					result.addAll(subs);
				}
			}
		}

		// 移除目录
		for (FileStatus f : lsRemoveFoldFile) {
			result.remove(f);
		}

		MRLog.info(LOG, "Total input paths to process : " + result.size());
		return result;
	}

	/**
	 * 获取子目录的文件
	 * 
	 * @param filestatus 文件状态
	 * @param job 配置对象
	 * @param inputFilter 文件过滤对象
	 * @return 满足要的文件列表
	 * @throws IOException IO异常
	 */
	public List<FileStatus> getSubFileStatus(FileStatus filestatus, JobContext job, PathFilter inputFilter)
			throws IOException {
		Path p = filestatus.getPath();
		FileSystem fs = p.getFileSystem(job.getConfiguration());
		FileStatus[] matches = fs.globStatus(p, inputFilter);
		if (matches == null || matches.length == 0) {
			return null;
		}

		List<FileStatus> result = new ArrayList<FileStatus>();
		for (FileStatus globStat : matches) {
			if (globStat.isDir()) {
				for (FileStatus stat : fs.listStatus(globStat.getPath(), inputFilter)) {
					result.add(stat);
				}
			} else {
				result.add(globStat);
			}
		}

		// 递归调用
		for (int i = 0; i < result.size(); i++) {
			FileStatus file = result.get(i);
			if (file.isDir()) {
				result.remove(i);
				List<FileStatus> subs = getSubFileStatus(file, job, inputFilter);
				if (subs != null) {
					result.addAll(subs);
				}
			}
		}

		return result;
	}

	/**
	 * Proxy PathFilter that accepts a path only if all filters given in the
	 * constructor do. Used by the listPaths() to apply the built-in
	 * hiddenFileFilter together with a user provided one (if any).
	 */
	private static class MultiPathFilter implements PathFilter {
		private List<PathFilter> filters;

		public MultiPathFilter(List<PathFilter> filters) {
			this.filters = filters;
		}

		public boolean accept(Path path) {
			for (PathFilter filter : filters) {
				if (!filter.accept(path)) {
					return false;
				}
			}
			return true;
		}
	}

	@Override
	public void handle(Job conf) throws Exception {
		FileConfiguration dbconf = new FileConfiguration(conf.getConfiguration(), FileConfiguration.FLAG_FILE_INPUT);
		// 源文件行分隔符
		String inputRowSplit = dbconf.getInputFileRowsSplitChars();
		if (null == inputRowSplit || inputRowSplit.length() <= 0) {
			MRLog.warn(LOG, "源文件行分隔符<" + FileConfiguration.INPUT_FILE_ROWS_SPLIT_CHARS + ">未设置");
		}

		// 源文件列分隔符
		String inputFieldSplit = dbconf.getInputFileFieldSplitChars();
		if (null == inputFieldSplit || inputFieldSplit.trim().length() <= 0) {
			MRLog.warn(LOG, "源文件列分隔符<" + FileConfiguration.INPUT_FILE_ROWS_SPLIT_CHARS + ">未设置");
		}

		// 获取是否启用文件过滤标识
		boolean pSysIsFilter = dbconf.getInputFileIsFilter();
		if (pSysIsFilter) {
			FileInputFormat.setInputPathFilter(conf, RegexPathFilter.class);
		}
	}

	/**
	 * 记录文件到文件列表中
	 * 
	 * @param fileSplit
	 * @param mrconf
	 */
	private void addFileToFileList(FileSplits fileSplit, MRConfiguration mrconf, String fileId) {
		String filePath = fileSplit.getPath().toUri().getPath().toString();
		MRFileListPO mfFilePo = MRLog.getInstance().getFilePoByFileId(fileId);
		if (null != mfFilePo) {
			return;
		}

		MRFileListPO file = new MRFileListPO();
		// 获取文件路径
		file.setDataSourceId(mrconf.getInputDataSourceId());
		file.setFileId(fileId);
		file.setFilePath(filePath);
		file.setFileSize(fileSplit.getLength());
		file.setRecordMonth(StringUtil.dateToString(new Date(), StringUtil.DATE_FORMAT_TYPE3));
		file.setRecordTime(StringUtil.dateToString(new Date(), StringUtil.DATE_FORMAT_TYPE1));
		// 记录文件到文件列表
		MRLog.getInstance().addFileListLog(file);
	}
}
