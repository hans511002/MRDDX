package com.ery.hadoop.mrddx.file;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
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

import com.ery.hadoop.mrddx.DBGroupReducer;
import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.FileMapper;
import com.ery.hadoop.mrddx.IHandleFormat;
import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.log.MRJobMapDataLog;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.mode.MRFileListPO;
import com.ery.hadoop.mrddx.util.StringUtil;
import com.ery.hadoop.mrddx.zk.MRFileZookeeper;

/**
 * 文件的输入格式
 * 



 * @createDate 2013-1-10
 * @version v1.0
 */
public class TextInputFormat extends org.apache.hadoop.mapreduce.lib.input.TextInputFormat implements IHandleFormat,
		Configurable {
	// 日志对象
	public static final Log LOG = LogFactory.getLog(TextInputFormat.class);

	// 输入文件的数量
	static final String NUM_INPUT_FILES = "mapreduce.input.num.files";
	private static final double SPLIT_SLOP = 1.1; // 10% slop
	private static final MRLog MRLOG = MRLog.getInstance();

	/**
	 * 构造方法
	 */
	public TextInputFormat() {
		// setMinSplitSize(SequenceFile.SYNC_INTERVAL);
	}

	private static final PathFilter hiddenFileFilter = new PathFilter() {
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".");
		}
	};

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
		// public RecordReader getRecordReader(InputSplit split, JobConf job,
		// Reporter reporter) throws IOException {
		// 日志
		long logId = context.getConfiguration().getLong(MRConfiguration.INTERNAL_JOB_LOG_ID, -1);
		MRLog.info(LOG, logId, "Get text recordReader");
		context.setStatus(split.toString());
		try {
			return new LineRecordReader(context.getConfiguration(), (FileSplit) split);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException {

		// public InputSplit[] getSplits(JobConf job, int numSplits) throws
		// IOException {
		List<FileStatus> files = listStatus(context);
		// Save the number of input files in the job-conf
		long totalSize = 0; // compute total size
		for (FileStatus file : files) { // check we have valid files
			if (file.isDir()) {
				throw new IOException("Not a file: " + file.getPath());
			}
			totalSize += file.getLen();
		}

		FileConfiguration fileconf = new FileConfiguration(context.getConfiguration());
		MRLOG.updateFileSizeJobRun(fileconf.getJobLogId(), totalSize);
		int numSplits = context.getConfiguration().getInt(MRConfiguration.MAPRED_MAP_TASKS, 0);
		int mapTasks = fileconf.getNumMapTasks();
		boolean isOneFileOneMap = fileconf.getOneFileOneMap();
		long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(context));
		long maxSize = getMaxSplitSize(context);

		String mapRaouteType = context.getConfiguration().get(MRConfiguration.SYS_MAP_RAOUTE_TYPE, ""); // map主机选择规则
		// host
		// map
		// default
		// none
		// 拆分文件
		List<InputSplit> splits = new ArrayList<InputSplit>(numSplits);
		String rowSplist = context.getConfiguration().get(FileConfiguration.INPUT_FILE_ROWS_SPLIT_CHARS, "\n"); // 获取行分隔符
		boolean nonSplit = rowSplist.endsWith("\n");
		for (FileStatus file : files) {
			Path path = file.getPath();
			FileSystem fs = path.getFileSystem(context.getConfiguration());
			long length = file.getLen();
			BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
			if (!isOneFileOneMap && (length != 0) && nonSplit && isSplitable(context, path)) {// 如果行分隔符的结尾是\n或者非压缩文件,否则按照文件数量拆分
				long blockSize = file.getBlockSize();
				long splitSize = computeSplitSize(blockSize, minSize, maxSize);
				long bytesRemaining = length;
				while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
					int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);
					splits.add(new FileSplits(path, length - bytesRemaining, splitSize, blkLocations[blkIndex]
							.getHosts()));
					bytesRemaining -= splitSize;
				}

				if (bytesRemaining != 0) {
					splits.add(new FileSplits(path, length - bytesRemaining, bytesRemaining,
							blkLocations[blkLocations.length - 1].getHosts()));
				}
			} else if (length != 0) {
				splits.add(new FileSplits(path, 0, length, blkLocations[0].getHosts()));
			} else {
				// Create empty hosts array for zero length files
				splits.add(new FileSplits(path, 0, length, new String[0]));
			}
			if (LOG.isDebugEnabled())
				MRLog.info(LOG, "File=>>" + file.getPath().toUri().toString());
		}

		MRLog.info(LOG, "Total of org splits: " + splits.size() + " begin checkFileSplit");
		this.checkFileSplit(fileconf, splits);
		MRLog.info(LOG, "end checkFileSplit ");
		this.unlockJOBIDZKNode(new MRConfiguration(context.getConfiguration()));
		MRLog.info(LOG, "Effective Total # of splits: " + splits.size());
		context.getConfiguration().setLong(NUM_INPUT_FILES, splits.size());
		try {
			List<InputSplit> endsplit = new LinkedList<InputSplit>();
			if (mapRaouteType.equals("default") && isOneFileOneMap) { // 一个map一个文件拆分
				List<InputSplit> tmpendsplit = groupHostSplits(splits);
				MRLog.info(LOG, "groupHostSplits.size:" + tmpendsplit.size());
				MRLog.info(LOG, "groupHostSplits:" + tmpendsplit);
				for (InputSplit inputSplit : tmpendsplit) {
					FileSplits fileSplits = (FileSplits) inputSplit;
					FileSplit fileSplit = new FileSplit(new Path[] { fileSplits.getPath() },
							new long[] { fileSplits.getStart() }, new long[] { fileSplits.getLength() },
							new String[] { fileSplits.getFileId() }, new long[] { fileSplits.getFileLogId() },
							fileSplits.getLocations());
					endsplit.add(fileSplit);
				}
			} else if (mapRaouteType.equals("host")) {
				hostNumSplit(splits, endsplit);
			} else if (mapRaouteType.equals("map")) {
				mapNumSplit(mapTasks, splits, endsplit);
			} else {
				// 按设定的MAP数组合
				int splitCount = 0;
				if (numSplits != 0) {
					List<List<InputSplit>> fileLsts = new ArrayList<List<InputSplit>>();
					for (int i = 0; i < numSplits; i++) {
						fileLsts.add(new ArrayList<InputSplit>());
					}
					for (InputSplit inputSplit : splits) {
						int index = splitCount % numSplits;
						splitCount++;
						List<InputSplit> idxLst = fileLsts.get(index);
						FileSplits fileSplits = (FileSplits) inputSplit;
						idxLst.add(fileSplits);
					}
					for (int i = 0; i < numSplits; i++) {
						List<InputSplit> idxLst = fileLsts.get(i);
						if (idxLst.size() > 0) {
							FileSplit fileSplit = new FileSplit(idxLst, null);// fileSplits.getHosts()
							endsplit.add(fileSplit);
						}
					}
				} else {
					for (InputSplit inputSplit : splits) {
						FileSplits fileSplits = (FileSplits) inputSplit;
						FileSplit fileSplit = new FileSplit(new Path[] { fileSplits.getPath() },
								new long[] { fileSplits.getStart() }, new long[] { fileSplits.getLength() },
								new String[] { fileSplits.getFileId() }, new long[] { fileSplits.getFileLogId() },
								(String[]) null);// fileSplits.getHosts()
						endsplit.add(fileSplit);
					}
				}
			}
			// ******************end 按制定map数拆分
			MRLog.info(LOG, " splits=" + endsplit);
			MRLog.info(LOG, "endsplit.size()=" + endsplit.size());
			context.getConfiguration().setLong(NUM_INPUT_FILES, splits.size());
			return endsplit;
		} catch (InterruptedException e) {
			LOG.error("优化拆分异常：", e);
			return splits;
			// throw new IOException(e);
		}
	}

	private void mapNumSplit(int mapTasks, List<InputSplit> splits, List<InputSplit> endsplit) throws IOException,
			InterruptedException {
		if (splits.size() <= 0) {
			return;
		}

		HashMap<String, List<InputSplit>> endHostList = groupHostSplit(splits);
		if (endHostList.size() == 1 && endHostList.containsKey("null")) {
			endsplit.addAll(endHostList.get("null"));
			return;
		}
		MRLog.info(LOG, "endHostList.size:" + endHostList.size());
		// MRLog.info(LOG, "endHostList:" + endHostList);
		HashMap<String, List<List<InputSplit>>> tempHostList = new HashMap<String, List<List<InputSplit>>>();
		int hostSize = endHostList.size();
		int mode = (mapTasks + hostSize - 1) / hostSize;

		for (String hosts : endHostList.keySet()) {
			List<List<InputSplit>> tempFileSplits = tempHostList.get(hosts);
			if (tempFileSplits == null) {
				tempFileSplits = new LinkedList<List<InputSplit>>();
				tempHostList.put(hosts, tempFileSplits);
				for (int i = 0; i < mode; i++)
					tempFileSplits.add(new LinkedList<InputSplit>());
			}
			List<InputSplit> lstSpt = endHostList.get(hosts);
			for (int i = 0; i < lstSpt.size(); i++) {
				tempFileSplits.get(i % mode).add(lstSpt.get(i));
			}
		}

		for (int i = 0; i < mode; i++) {
			for (String hosts : tempHostList.keySet()) {
				List<List<InputSplit>> lst = tempHostList.get(hosts);
				if (lst.size() > 0) {
					List<InputSplit> lstSplit = lst.remove(0);
					if (lstSplit.size() > 0) {
						endsplit.add(new FileSplit(lstSplit, hosts));
					}
				}
			}
		}

		MRLog.info(LOG, "endSplitsList:" + endsplit);
		// for (String hosts : endHostList.keySet()) {
		// List<InputSplit> tempFileSplits = tempHostList.get(hosts);
		// if (tempFileSplits == null){
		// tempFileSplits = new LinkedList<InputSplit>();
		// tempHostList.put(hosts, tempFileSplits);
		// }
		// List<InputSplit> lstSpt = endHostList.get(hosts);
		// int perSize = lstSpt.size()/mode;
		// int remSize = lstSpt.size()%mode;
		// // System.out.println("perSize=" + perSize + "  remSize="+remSize
		// +" lstSpt.size()="+lstSpt.size());
		// int perIndex = 0;
		// for (int i = 0; i < mode; i++) {
		// // System.out.print("第"+i+"组");
		// int len = perSize;
		// if (remSize>0){
		// len = len+1;
		// remSize--;
		// }
		//
		// if (i == (mode-1) && remSize>0){// 处理最后剩余
		// len += remSize;
		// }
		//
		// if (len <= 0){
		// break;
		// }
		// // System.out.println(" len="+len);
		// Path tmpPath[] = new Path[len];
		// long tmpstart[] = new long[len];
		// long tmplength[] = new long[len];
		// long tmpFileLogId[] = new long[len];
		// String tmpFileId[] = new String[len];
		// int tmpIndex = 0;
		// // System.out.println("perIndex="+perIndex);
		// for (int j = perIndex; j < perIndex + len; j++) {
		// FileSplits fileSplits = (FileSplits)lstSpt.get(j);
		// //
		// System.out.println("           fileSplits="+fileSplits.toString());
		// tmpPath[tmpIndex] = fileSplits.getPath();
		// tmpstart[tmpIndex] = fileSplits.getStart();
		// tmplength[tmpIndex] = fileSplits.getLength();
		// tmpFileLogId[tmpIndex] = fileSplits.getFileLogId();
		// tmpFileId[tmpIndex] = fileSplits.getFileId();
		// tmpIndex++;
		// }
		// perIndex = perIndex + len;
		//
		// FileSplit fileSplit = new FileSplit(tmpPath, tmpstart, tmplength, new
		// String[]{hosts});
		// fileSplit.setFileId(tmpFileId);
		// fileSplit.setFileLogId(tmpFileLogId);
		// tempFileSplits.add(fileSplit);
		// }
		// }
		//
		// // 添加到拆分列表中
		// for (String hosts : tempHostList.keySet()) {
		// List<InputSplit> lst = tempHostList.get(hosts);
		// endsplit.addAll(lst);
		// }
	}

	private List<InputSplit> groupHostSplits(List<InputSplit> splits) throws IOException, InterruptedException {
		HashMap<String, List<InputSplit>> hostsSplit = groupHostSplit(splits);
		if (hostsSplit.size() == 1 && hostsSplit.containsKey("null")) {
			return hostsSplit.get("null");
		}
		Map<String, Integer> hosts = new HashMap<String, Integer>();
		List<InputSplit> res = new ArrayList<InputSplit>();
		Map<String, Integer> minus = new HashMap<String, Integer>();
		int index = 0;
		while (true) {
			index++;// 轮次
			for (String host : hostsSplit.keySet()) {// 按主机循环
				List<InputSplit> hsp = hostsSplit.get(host);
				Integer size = hosts.get(host);
				if (size == null) {
					size = 0;
				}
				if (hsp.size() > 0) {
					if (size >= index && minus.size() > 0) {
						FileSplits is = (FileSplits) hsp.get(size);
						boolean added = false;
						for (String mhost : is.getLocations()) {
							if (minus.containsKey(mhost)) {
								Integer _size = minus.remove(mhost);
								size = hosts.get(mhost);
								hosts.put(mhost, size + 1);
								res.add(_size, is.clone(new String[] { mhost }, null));
								added = true;
								break;
							}
						}
						if (!added) {
							is = (FileSplits) hsp.remove(size.intValue());
							hsp.add(size, is.clone(new String[] { host }, null));
							hosts.put(host, size + 1);
						}
					} else {
						FileSplits isold = (FileSplits) hsp.remove(0);
						res.add(isold.clone(new String[] { host }, null));
						hosts.put(host, size + 1);
					}
				} else {
					minus.put(host, res.size());
				}
			}
			if (res.size() >= splits.size()) {
				break;
			}
		}
		return res;
	}

	private HashMap<String, List<InputSplit>> groupHostSplit(List<InputSplit> splits) throws IOException,
			InterruptedException {
		HashMap<String, Integer> hostsSplits = new HashMap<String, Integer>();
		for (InputSplit split : splits) {
			String lcs[] = split.getLocations();
			if (lcs != null && lcs.length > 0) {
				for (String lc : lcs) {
					Integer hsp = hostsSplits.get(lc);
					hostsSplits.put(lc, hsp == null ? 1 : hsp + 1);
				}
			}
		}
		HashMap<String, List<InputSplit>> hostsSplit = new HashMap<String, List<InputSplit>>();
		if (hostsSplits.size() > 0) {
			for (InputSplit split : splits) {
				List<InputSplit> minList = getMinHosts(hostsSplit, split.getLocations());
				minList.add(split);
			}
		} else {
			hostsSplit.put("null", splits);
		}
		return hostsSplit;
	}

	private List<InputSplit> getMinHosts(HashMap<String, List<InputSplit>> hostsSplit, String[] hosts) {
		List<InputSplit> minList = null;
		for (String host : hosts) {
			// if ("scdbdatanode14".equals(host) ||
			// "scdbdatanode15".equals(host)) {
			// continue;
			// }

			if (minList == null) {
				minList = hostsSplit.get(host);
				if (minList == null) {
					minList = new ArrayList<InputSplit>();
					hostsSplit.put(host, minList);
				}
			} else {
				List<InputSplit> curList = hostsSplit.get(host);
				if (curList == null) {
					curList = new ArrayList<InputSplit>();
					hostsSplit.put(host, curList);
				} else if (curList.size() < minList.size()) {
					minList = curList;
				}
			}
		}
		return minList;
	}

	private void hostNumSplit(List<InputSplit> splits, List<InputSplit> endsplit) throws IOException,
			InterruptedException {
		HashMap<String, List<InputSplit>> endHostList = groupHostSplit(splits);
		if (endHostList.size() == 1 && endHostList.containsKey("null")) {
			endsplit.addAll(endHostList.get("null"));
			return;
		}
		MRLog.info(LOG, "endHostList.size:" + endHostList.size());
		for (String hosts : endHostList.keySet()) {
			List<InputSplit> lstInputSplit = endHostList.get(hosts);
			for (InputSplit inputSplit : lstInputSplit) {
				String tmphosts[] = inputSplit.getLocations();
				tmphosts[0] = hosts;
			}
		}

		for (String hosts : endHostList.keySet()) {
			List<InputSplit> lstSpt = endHostList.get(hosts);
			int len = lstSpt.size();
			Path tmpPath[] = new Path[len];
			long tmpstart[] = new long[len];
			long tmplength[] = new long[len];
			long tmpFileLogId[] = new long[len];
			String tmpFileId[] = new String[len];
			for (int i = 0; i < lstSpt.size(); i++) {
				FileSplits fileSplits = (FileSplits) lstSpt.get(i);
				tmpPath[i] = fileSplits.getPath();
				tmpstart[i] = fileSplits.getStart();
				tmplength[i] = fileSplits.getLength();
				tmpFileLogId[i] = fileSplits.getFileLogId();
				tmpFileId[i] = fileSplits.getFileId();
			}
			FileSplit fileSplit = new FileSplit(tmpPath, tmpstart, tmplength, tmpFileId, tmpFileLogId,
					new String[] { hosts });
			endsplit.add(fileSplit);
		}
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
			for (InputSplit inputSplit : splits) {
				// 根据jobid+文件id查询最大的日志id
				FileSplits fileSplit = (FileSplits) inputSplit;
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
			for (InputSplit inputSplit : splits) {
				FileSplits fileSplit = (FileSplits) inputSplit;
				// 根据jobid+文件id查询最大的日志id
				Path path = fileSplit.getPath();
				String filePath = path.toUri().getPath().toString();
				String fileId = StringUtil.getFileOnlyKey(filePath, fileconf.getInputDataSourceId());
				if (LOG.isDebugEnabled())
					MRLog.debug(LOG, "jobId=" + fileconf.getSysJobId() + " , fileId=" + fileId);
				MRJobMapDataLog dataLog = MRLOG.getMRJobMapDataLog(fileconf.getSysJobId(), fileId);// 查询处理日志信息.
				// 检查文件是否存在，不存在加入文件表中
				this.addFileToFileList(fileSplit, fileconf, fileId);
				if (LOG.isDebugEnabled())
					MRLog.debug(LOG, "dataLog=" + dataLog);
				if (null == dataLog) {
					this.initMRDataLog(fileSplit, fileconf, filePath, fileId, fileconf.getInputDataSourceId());
					continue;
				}
				if (LOG.isDebugEnabled())
					MRLog.debug(LOG, "dataLog.getStatus()=" + dataLog.getStatus());
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
		Configuration conf = fileconf.getConf();
		// String hdfsAddress = mrJobMapDataLog.getHdfsAddress();
		// if (null == hdfsAddress || hdfsAddress.trim().length()<=0){
		// return null;
		// }
		// conf.set(MRConfiguration.FS_DEFAULT_NAME, hdfsAddress);

		String path = mrJobMapDataLog.getFilePath();
		Path hdfsPath = new Path(path);
		FileSystem fs = hdfsPath.getFileSystem(conf);
		FileStatus file = fs.getFileStatus(hdfsPath);
		if (fs.exists(hdfsPath) && fs.isFile(hdfsPath) && !file.isDir()) {
			long length = file.getLen();
			BlockLocation[] blkLocations = null;
			if (file instanceof LocatedFileStatus) {
				blkLocations = ((LocatedFileStatus) file).getBlockLocations();
			} else {
				blkLocations = fs.getFileBlockLocations(file, 0, length);
			}
			if (length != 0) {// 按文件数量拆分
				int blkIndex = getBlockIndex(blkLocations, 0);
				return new FileSplits(file.getPath(), 0l, length, blkLocations[blkIndex].getHosts(),
						blkLocations[blkIndex].getCachedHosts());
			} else {
				// Create empty hosts array for zero length files
				return new FileSplits(file.getPath(), 0l, length, new String[0]);
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
	protected List<FileStatus> listStatus(JobContext context) throws IOException {
		Configuration job = context.getConfiguration();
		Path[] dirs = getInputPaths(context);
		if (dirs.length == 0) {
			throw new IOException("No input paths specified in job");
		}

		// get tokens for all the required FileSystems..
		TokenCache.obtainTokensForNamenodes(context.getCredentials(), dirs, job);

		List<FileStatus> result = new ArrayList<FileStatus>();
		List<IOException> errors = new ArrayList<IOException>();

		// creates a MultiPathFilter with the hiddenFileFilter and the
		// user provided one (if any).
		List<PathFilter> filters = new ArrayList<PathFilter>();
		filters.add(hiddenFileFilter);
		PathFilter jobFilter = getInputPathFilter(context);
		if (jobFilter != null) {
			filters.add(jobFilter);
		}
		PathFilter inputFilter = new MultiPathFilter(filters);
		for (Path p : dirs) {
			FileSystem fs = p.getFileSystem(job);
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
	 * @param filestatus
	 *            文件状态
	 * @param job
	 *            配置对象
	 * @param inputFilter
	 *            文件过滤对象
	 * @return 满足要的文件列表
	 * @throws IOException
	 *             IO异常
	 */
	public List<FileStatus> getSubFileStatus(FileStatus filestatus, Configuration job, PathFilter inputFilter)
			throws IOException {
		Path p = filestatus.getPath();
		FileSystem fs = p.getFileSystem(job);
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
		org.apache.hadoop.mapreduce.lib.input.TextInputFormat.setMinInputSplitSize(conf, SequenceFile.SYNC_INTERVAL);
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

		conf.setMapOutputKeyClass(DBRecord.class);
		conf.setMapOutputValueClass(DBRecord.class);
		conf.setInputFormatClass(TextInputFormat.class);
		if (dbconf.getInputIsCombiner()) {
			conf.setCombinerClass(DBGroupReducer.class);
		}

		conf.setMapperClass(FileMapper.class);
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

	Configuration conf = null;

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;

	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}

}
