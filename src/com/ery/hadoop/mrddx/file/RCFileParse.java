package com.ery.hadoop.mrddx.file;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetworkTopology;

import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.log.MRLog;

/**
 * @author wanghao
 * 
 */
public class RCFileParse extends AbstractFileParse {
	// 日志对象
	public static final Log LOG = LogFactory.getLog(RCFileParse.class);
	private String[] srcFieldNames;

	public RCFileParse() {
	}

	public RCFileParse(String[] srcFieldNames) {
		this.srcFieldNames = srcFieldNames;
	}

	public DBRecord parseToDBRecord(BytesRefArrayWritable val) throws IOException {
		String[] fields = new String[val.size()];
		Text txt = new Text();
		// RcFile行存储和列存储，每次一行数据，val是个列簇，遍历，输出
		for (int i = 0; i < val.size(); i++) {
			BytesRefWritable v = val.get(i);
			txt.set(v.getData(), v.getStart(), v.getLength());
			fields[i] = txt.toString();
		}

		// 转化文本数据为Mapper的输出的key-value对的格式(DBRecord)
		return new DBRecord(this.srcFieldNames, fields);
	}

	/**
	 * 验证文件是否为RC
	 * 
	 * @param conf
	 *            配置对象
	 * @param fileStatus
	 *            文件状态列表
	 * @return 是否满足RCFile的格式
	 * @throws IOException
	 */
	public boolean validateInputRCFile(Configuration job, List<FileStatus> fileStatus) throws IOException {
		if (fileStatus == null || fileStatus.size() <= 0) {
			return false;
		}
		for (FileStatus fileStatus2 : fileStatus) {
			Path path = fileStatus2.getPath();
			if (!validateRCFile(job, path)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * 验证是否为rc文件
	 * 
	 * @param job
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public boolean validateRCFile(Configuration job, Path path) throws IOException {
		FileSystem fs = path.getFileSystem(job);
		RCFile.Reader reader = null;
		try {
			reader = new RCFile.Reader(fs, path, job);
			reader.close();
			reader = null;
		} catch (IOException e) {
			MRLog.errorException(LOG, "文件:" + path.toUri().getPath() + " 不满足RCFILE", e);
			return false;
		} finally {
			if (null != reader) {
				reader.close();
			}
		}
		return true;
	}

	@Override
	public String[] getSplitHosts(BlockLocation[] blkLocations, long offset, long splitSize, NetworkTopology clusterMap)
			throws IOException {
		return super.getSplitHosts(blkLocations, offset, splitSize, clusterMap);
	}
}
