package com.ery.hadoop.mrddx.file;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.DBReducer;
import com.ery.hadoop.mrddx.IHandleFormat;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.HDFSUtils;

/**
 * 文本输出格式
 * 



 * @createDate 2013-1-10
 * @version v1.0
 * @param <K>
 * @param <V>
 */
public class TextOutputFormat<K, V> extends FileOutputFormat<K, V> implements IHandleFormat {
	// 日志对象
	public static final Log LOG = LogFactory.getLog(TextInputFormat.class);

	/**
	 * 写行记录
	 * 



	 * @createDate 2013-1-10
	 * @version v1.0
	 * @param <K>
	 * @param <V>
	 */
	protected static class LineRecordWriter<K, V> extends RecordWriter<K, V> {
		// 初始行数
		private int rowCount = 0;
		private static final String utf8 = "UTF-8";
		private final byte[] newline;
		protected DataOutputStream out;
		private final byte[] fieldSeparator;
		private String[] fieldNames;

		public LineRecordWriter(DataOutputStream out) {
			this(out, ",", "\n");
		}

		public LineRecordWriter(DataOutputStream out, String fieldSeparator) {
			this(out, fieldSeparator, "\n");
		}

		public LineRecordWriter(DataOutputStream out, String fieldSeparator, String rowSeparator) {
			this(out, fieldSeparator, rowSeparator, null);
		}

		public LineRecordWriter(DataOutputStream out, String fieldSeparator, String rowSeparator, String[] fieldNames) {
			this.out = out;
			try {
				this.newline = rowSeparator.getBytes(utf8);
				this.fieldSeparator = fieldSeparator.getBytes(utf8);
				this.fieldNames = fieldNames;
			} catch (UnsupportedEncodingException uee) {
				throw new IllegalArgumentException("can't find " + utf8 + " encoding");
			}
		}

		/**
		 * Write the object to the byte stream, handling Text as a special case.
		 * 
		 * @param o
		 *            the object to print
		 * @throws IOException
		 *             if the write throws, we pass it on
		 */
		private void writeObject(Object o) throws IOException {
			if (o instanceof Text) {
				Text to = (Text) o;
				this.out.write(to.getBytes(), 0, to.getLength());
			} else {
				this.out.write(o.toString().getBytes(utf8));
			}
		}

		public synchronized void write(K key, V value) throws IOException {
			boolean nullKey = key == null || key instanceof NullWritable;
			boolean nullValue = value == null || value instanceof NullWritable;
			if (nullKey && nullValue) {
				return;
			}

			if (!nullKey && key instanceof DBRecord) {
				// 未指定字段，第一行写入字段
				if (this.fieldNames == null) {
					this.fieldNames = ((DBRecord) key).getFieldNames();
					this.out.writeChars("COLUMN_NAMES:");
					for (int i = 0; i < this.fieldNames.length; i++) {
						if (i > 0) {
							this.out.write(this.fieldSeparator);
						}
						this.out.writeChars(this.fieldNames[i]);
					}
					this.out.write(this.newline);
				}

				((DBRecord) key).write(this.out, this.fieldNames, this.fieldSeparator, this.newline);
				this.rowCount++;
				if (rowCount % 200000 == 0) {
					LOG.info("insert " + rowCount + " rows");
					// MRLog.consoleDebug(LOG, "insert " + rowCount + " rows");
				}
			} else {
				if (!nullKey) {
					writeObject(key);
				}

				if (!(nullKey || nullValue)) {
					this.out.write(this.fieldSeparator);
				}

				if (!nullValue) {
					writeObject(value);
				}
				this.out.write(this.newline);
			}
		}

		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			this.out.close();
		}
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		// public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf
		// job, String name, Progressable progress)
		// throws IOException {
		Configuration job = context.getConfiguration();
		FileConfiguration dbConf = new FileConfiguration(job, FileConfiguration.FLAG_FILE_OUTPUT);
		boolean isCompressed = dbConf.getOutputFileCompress();
		String fieldSeparator = dbConf.getOutputFileFieldSplitChars();
		String rowSeparator = dbConf.getOutputFileRowsSplitChars();
		String[] fieldNames = dbConf.getOutputFieldNames();
		if (!isCompressed) {
			Path file = this.getDefaultWorkFile(context, "");
			// Path file = FileOutputFormat.getTaskOutputPath(job, name);
			FileSystem fs = file.getFileSystem(job);
			FSDataOutputStream fileOut = fs.create(file, context);
			return new LineRecordWriter<K, V>(fileOut, fieldSeparator, rowSeparator, fieldNames);
		} else {
			// 需要压缩
			// 获取压缩标识
			// create the named codec
			String compresseCodec = dbConf.getOutputFileCompressCodec();
			CompressionCodec codec = HDFSUtils.getCompressCodec(compresseCodec, job);
			// build the filename including the extension
			Path file = this.getDefaultWorkFile(context, codec.getDefaultExtension());
			// Path file = FileOutputFormat.getTaskOutputPath(job, name +
			// codec.getDefaultExtension());
			FileSystem fs = file.getFileSystem(job);
			FSDataOutputStream fileOut = fs.create(file, context);
			return new LineRecordWriter<K, V>(new DataOutputStream(codec.createOutputStream(fileOut)), fieldSeparator,
					rowSeparator, fieldNames);
		}
	}

	/**
	 * 设置输出参数
	 * 
	 * @param conf
	 * @param pOutputCompress
	 *            是否被压缩
	 * @param pOutputCompressCodec
	 *            压缩方式
	 * @param pOutputFileFieldSplitChars
	 *            列分隔符
	 * @param pOutputFileRowsSplitChars
	 *            行分隔符
	 * @throws Exception
	 */
	public static void setOutputParameter(Job conf, boolean pOutputCompress, String pOutputCompressCodec,
			String pOutputFileFieldSplitChars, String pOutputFileRowsSplitChars) throws Exception {
		FileConfiguration dbconf = new FileConfiguration(conf.getConfiguration(), FileConfiguration.FLAG_FILE_OUTPUT);
		dbconf.setOutputFileCompress(pOutputCompress);
		dbconf.setOutputFileCompressCodec(pOutputCompressCodec);
		dbconf.setOutputFileFieldSplitChars(pOutputFileFieldSplitChars);
		dbconf.setOutputFileRowsSplitChars(pOutputFileRowsSplitChars);
	}

	@Override
	public void handle(Job conf) throws Exception {
		/**
		 * 校验参数
		 */
		FileConfiguration dbconf = new FileConfiguration(conf.getConfiguration(), FileConfiguration.FLAG_FILE_OUTPUT);
		// 获取目标文件行分隔符
		String outRowChars = dbconf.getOutputFileRowsSplitChars();
		if (null == outRowChars || outRowChars.length() <= 0) {
			MRLog.warn(LOG, "输出行分隔符<" + FileConfiguration.OUTPUT_FILE_ROWS_SPLIT_CHARS + ">未设置,默认\n");
		}

		// 目标文件列分隔符
		String outFileSplitChars = dbconf.getOutputFileFieldSplitChars();
		if (null == outFileSplitChars || outFileSplitChars.trim().length() <= 0) {
			String meg = "输出行列隔符<" + FileConfiguration.OUTPUT_FILE_FIELD_SPLIT_CHARS + ">未设置";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		boolean para = dbconf.getOutputFileCompress();
		// 目标文件压缩方式 (压缩格式：HDFSUtils.CompressCodec)
		String outCompressCodec = dbconf.getOutputFileCompressCodec();
		if (para && !HDFSUtils.isExistCompressCodec(outCompressCodec)) {
			String meg = "[MR ERROR]目标文件压缩方式<" + FileConfiguration.OUTPUT_FILE_COMPRESSCODEC + ">设置不正确.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		// 获取MR最终存放结果的目录
		String outTargetpath = dbconf.getOutputTargetFilePath();
		dbconf.setOutputTargetPath(outTargetpath);
		if (null == outTargetpath || outTargetpath.trim().length() <= 0) {
			MRLog.warn(LOG, "MR最终存放结果的目录<" + FileConfiguration.OUTPUT_FILE_TARGET_PATH + ">为空");
		}

		conf.setOutputKeyClass(DBRecord.class);
		conf.setOutputValueClass(NullWritable.class);
		conf.setOutputFormatClass(TextOutputFormat.class);
		conf.setReducerClass(DBReducer.class);
	}
}
