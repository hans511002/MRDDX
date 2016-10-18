package com.ery.hadoop.mrddx.file;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.util.HDFSUtils;

/**
 * RCFile文件的输出类
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-1-14
 * @version v1.0
 * @param <K>
 * @param <V>
 */
public class RCFileRecordWriter<K, V> extends RecordWriter<K, V> {
	// 日志对象
	public static final Log LOG = LogFactory.getLog(RCFileRecordWriter.class);

	// 初始行数
	private int rowCount = 0;

	// 写对象
	private RCFile.Writer out;

	// 字段列表
	private String[] fieldNames;

	// 字段分隔符
	private String fieldSplitChars;

	// 行分隔符
	private String rowSplitChars;

	/**
	 * 默认构造方法
	 */
	public RCFileRecordWriter() {
	}

	/**
	 * 构造方法
	 * 
	 * @param fs
	 *            文件系统对象
	 * @param job
	 *            job配置对象
	 * @param file
	 *            文件path对象
	 * @param progress
	 *            进度对象
	 * @param codec
	 *            压缩对象
	 * @param fieldNames
	 *            字段名称
	 * @param fieldSplitChars
	 *            字段分隔符
	 * @param rowSplitChars
	 *            行分隔符
	 * @throws IOException
	 *             IO异常
	 */
	public RCFileRecordWriter(TaskAttemptContext context, FileOutputFormat fileOutputFormat) throws IOException {
		FileConfiguration dbConf = new FileConfiguration(context.getConfiguration(), FileConfiguration.FLAG_FILE_OUTPUT);
		boolean isCompressed = dbConf.getOutputFileCompress();
		Path file = fileOutputFormat.getDefaultWorkFile(context, "");
		// FileOutputFormat.getTaskOutputPath(job, name);
		FileSystem fs = file.getFileSystem(context.getConfiguration());
		CompressionCodec codec = null;
		if (isCompressed) {
			// find the right codec
			String compresseCodec = dbConf.getOutputFileCompressCodec();
			codec = HDFSUtils.getCompressCodec(compresseCodec, context.getConfiguration());
		}
		this.fieldSplitChars = dbConf.getOutputFileFieldSplitChars();
		this.rowSplitChars = dbConf.getOutputFileRowsSplitChars();
		this.fieldNames = dbConf.getOutputFieldNames();
		this.out = new RCFile.Writer(fs, context.getConfiguration(), file, context, codec);
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		this.out.close();
	}

	@Override
	public void write(K key, V value) throws IOException {
		boolean nullKey = key == null || key instanceof NullWritable;
		boolean nullValue = value == null || value instanceof NullWritable;
		if (nullKey && nullValue) {
			return;
		}

		if (!nullKey && key instanceof DBRecord) {
			((DBRecord) key).write(this.out, this.fieldNames, this.fieldSplitChars, this.rowSplitChars);
			this.rowCount++;
			if (rowCount % 200000 == 0) {
				LOG.info("insert " + rowCount + " rows");
				// MRLog.consoleDebug(LOG, "insert " + rowCount + " rows");
			}
		}
	}
}
