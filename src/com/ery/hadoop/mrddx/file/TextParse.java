package com.ery.hadoop.mrddx.file;

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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetworkTopology;

import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.log.MRLog;

public class TextParse extends AbstractFileParse {

	public static final MRLog MRLOG = MRLog.getInstance();
	public static final Log LOG = LogFactory.getLog(TextParse.class);

	// 任务ID
	private String taskId;

	// 输入字段名称
	private String[] srcFieldNames;

	// 拆分列的分隔符
	private String fieldSplitChars;

	// 拆分行记录分隔符
	private String rowSplitChars;

	// 临时存放数据
	private String tempRecordValue = "";

	// 临时存放记录数队列
	private List<DBRecord> recordLst;

	public TextParse() {

	}

	public TextParse(String[] srcFieldNames, String fieldSplitChars, String rowSplitChars, String taskId) {
		this.srcFieldNames = srcFieldNames;
		this.fieldSplitChars = fieldSplitChars;
		this.rowSplitChars = rowSplitChars;
		this.taskId = taskId;
	}

	/**
	 * 处理文本转为DBRecord的方法
	 * 
	 * @param val
	 *            文本数据
	 * @return DBRecord记录
	 * @throws IOException
	 *             IO异常
	 */
	public DBRecord[] parseToDBRecord(Text val) throws IOException {
		return parseToDBRecord(val.toString());
	}

	public DBRecord[] parseToDBRecord(String value) throws IOException {
		if (value == null || value.trim().length() <= 0) {
			return null;
		}

		if (this.srcFieldNames == null) {
			String msg = "未指定输入字段列表";
			MRLOG.jobMapRunMsg(this.taskId, MRLog.LOG_TYPE_ERROR, new Date(), msg);
			throw new IOException(msg);
		}

		// 补全换行符
		this.tempRecordValue = this.tempRecordValue + value + "\n";
		int end = this.tempRecordValue.indexOf(this.rowSplitChars);

		// 如果未找到分隔符，返回
		if (end == -1) {
			return null;
		}

		if (null == this.recordLst) {
			this.recordLst = new ArrayList<DBRecord>();
		}

		this.recordLst.clear();
		while (end >= 0) {
			// 如果分隔符在字符串开头,则去掉开头分隔符
			if (end == 0) {
				this.tempRecordValue = this.tempRecordValue.substring(this.rowSplitChars.length(),
						this.tempRecordValue.length());
				end = this.tempRecordValue.indexOf(rowSplitChars);
			} else if (end > 0) {
				// 获取一条记录
				String srecord = this.tempRecordValue.substring(0, end);
				// 截取余下内容
				this.tempRecordValue = this.tempRecordValue.substring(end, this.tempRecordValue.length());
				// 查找下一个行分隔符
				end = this.tempRecordValue.indexOf(this.rowSplitChars);
				// 如果记录为空，继续循环
				if (srecord.trim().length() <= 0) {
					continue;
				}

				// 存放文本数据到DBRecord中（传入的输入字段srcfieldNames与value分割后的索引顺序一致）
				String[] values = srecord.split(this.fieldSplitChars);
				DBRecord row = new DBRecord(this.srcFieldNames, values);
				this.recordLst.add(row);
			}
		}

		return this.recordLst.toArray(new DBRecord[0]);
	}

	@Override
	public String[] getSplitHosts(BlockLocation[] blkLocations, long offset, long splitSize, NetworkTopology clusterMap)
			throws IOException {
		return super.getSplitHosts(blkLocations, offset, splitSize, clusterMap);
	}

	/**
	 * 验证文件是否为SEQ
	 * 
	 * @param conf
	 *            配置对象
	 * @param fileStatus
	 *            文件状态列表
	 * @return 是否满足SEQ的格式
	 * @throws IOException
	 */
	public boolean validateInputSEQ(Configuration job, List<FileStatus> fileStatus) throws IOException {
		if (fileStatus == null || fileStatus.size() <= 0) {
			return false;
		}
		for (FileStatus fileStatus2 : fileStatus) {
			Path path = fileStatus2.getPath();
			if (!validateSeqFile(job, path)) {
				return false;
			}
		}
		return true;
	}

	public boolean validateSeqFile(Configuration job, Path path) throws IOException {
		FileSystem fs = path.getFileSystem(job);
		SequenceFile.Reader seqReader = null;
		try {
			seqReader = new SequenceFile.Reader(fs, path, job);
			seqReader.close();
			seqReader = null;
		} catch (IOException e) {
			MRLog.errorException(LOG, "文件:" + path.toUri().getPath() + " 不满足SEQ", e);
			return false;
		} finally {
			if (null != seqReader) {
				seqReader.close();
			}
		}

		return true;
	}

}
