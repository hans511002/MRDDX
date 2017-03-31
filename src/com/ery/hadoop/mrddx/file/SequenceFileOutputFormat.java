package com.ery.hadoop.mrddx.file;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
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
 * Sequence文件输出格式
 * 



 * @createDate 2013-1-10
 * @version v1.0
 * @param <K>
 * @param <V>
 */
public class SequenceFileOutputFormat<K, V> extends FileOutputFormat<K, V> implements IHandleFormat {
	// 日志对象
	public static final Log LOG = LogFactory.getLog(SequenceFileOutputFormat.class);

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		// public RecordWriter<K, V> getRecordWriter(FileSystem ignored, Job
		// job, String name, Progressable progress)
		// throws IOException {
		Configuration job = context.getConfiguration();
		FileConfiguration dbConf = new FileConfiguration(context.getConfiguration(), FileConfiguration.FLAG_FILE_OUTPUT);
		boolean isCompressed = dbConf.getOutputFileCompress();
		final String fieldSplitChars = dbConf.getOutputFileFieldSplitChars();
		final String rowSplitChars = dbConf.getOutputFileRowsSplitChars();
		final String[] fieldNames = dbConf.getOutputFieldNames();

		// get the path of the temporary output file
		Path file = this.getDefaultWorkFile(context, "");
		FileSystem fs = file.getFileSystem(context.getConfiguration());
		CompressionCodec codec = null;
		CompressionType compressionType = CompressionType.NONE;
		if (isCompressed) {
			// find the kind of compression to do
			String compresseTypeVar = dbConf.getOutputFileCompressCodecType();
			compressionType = CompressionType.valueOf(compresseTypeVar);
			// find the right codec
			String compresseCodec = dbConf.getOutputFileCompressCodec();
			codec = HDFSUtils.getCompressCodec(compresseCodec, job);
		}

		final SequenceFile.Writer out = SequenceFile.createWriter(fs, job, file, context.getOutputKeyClass(),
				context.getOutputValueClass(), compressionType, codec, context);
		return new RecordWriter<K, V>() {
			// 初始行数
			private int rowCount = 0;

			public void write(K key, V value) throws IOException {
				boolean nullKey = key == null || key instanceof NullWritable;
				boolean nullValue = value == null || value instanceof NullWritable;
				if (nullKey && nullValue) {
					return;
				}

				if (!nullKey && key instanceof DBRecord) {
					((DBRecord) key).write(out, fieldNames, fieldSplitChars, rowSplitChars);
					this.rowCount++;
					if (rowCount % 200000 == 0) {
						LOG.info("insert " + rowCount + " rows");
						// MRLog.consoleDebug(LOG, "insert " + rowCount +
						// " rows");
					}
				}
			}

			@Override
			public void close(TaskAttemptContext context) throws IOException, InterruptedException {
				out.close();
			}
		};
	}

	/**
	 * 设置输出参数
	 * 
	 * @param conf
	 *            配置对象
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
			String meg = "输出行分隔符<" + FileConfiguration.OUTPUT_FILE_ROWS_SPLIT_CHARS + ">未设置";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
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

		conf.setOutputFormatClass(SequenceFileOutputFormat.class);
		conf.setReducerClass(DBReducer.class);
	}

}