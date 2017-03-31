package com.ery.hadoop.mrddx.hive;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.db.mapreduce.FileWritable;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.HDFSUtils;
import com.ery.hadoop.mrddx.util.StringUtil;

/**
 * Hive针对RCFile的输出类
 * 



 * @createDate 2013-2-21
 * @version v1.0
 * @param <K>
 * @param <V>
 */
@InterfaceStability.Evolving
public class HiveRCFileRecordWriter<K extends FileWritable, V> extends RecordWriter<K, V> {
	// 日志对象
	private static final Log LOG = LogFactory.getLog(HiveRCFileRecordWriter.class);

	// 初始行数
	private int rowCount = 0;

	private Configuration job;

	// 输出流
	private RCFile.Writer out;

	// 输出的字段分隔符
	private String fieldSeparator;

	// 输出的行分隔符
	private String rowSeparator;

	// 输出对应的字段名称
	private String[] fieldNames;

	// 是否分区输出，true表示非分区输出
	private boolean isNotPartitionsFistRow = true;

	/**
	 * 针对直接生成文件功能
	 */
	// 分区字段名称
	private String[] partitionField;
	// 分区输出的临时路径
	private String orderOutputTmpPath;
	// hive表名
	private String tableName;
	// 分区输出的最终路径
	private String orderOutputPath;
	// 分区输出的文件名称
	private String orderOutputFileNamePrefix;
	// 分区输出的缓存大小
	@SuppressWarnings("unused")
	private int orderWriteBufferSize;
	// 分区输出的文件MAX COUNT(单位:条数)，默认：-1,不限制
	private long orderOutputFileMaxCount;
	// 分区输出流列表,分区目录与实际的文件目录
	private Map<String, String> lstPartitionFilePath;
	// 分区输出流列表,实际的文件目录与输出流
	private Map<String, RCFile.Writer> lstPartitionOutOS;
	// 分区输出流列表,实际的文件目录与记录数
	private Map<String, Long> lstPartitionFileCount;
	// 文件的游标
	private int ticket = 0;
	HiveConfiguration hiveConf = null;

	/**
	 * 构造方法
	 * 
	 * @param job
	 * 
	 * @param out
	 *            输出流
	 * @param fieldSeparator
	 *            输出的字段分隔符
	 * @param rowSeparator
	 *            输出的行分隔符
	 * @param fieldNames
	 *            输出对应的字段名称
	 * @throws IOException
	 */
	public HiveRCFileRecordWriter(TaskAttemptContext context, FileOutputFormat inputOutputFormat) throws IOException {
		this.job = context.getConfiguration();
		// 分区参数
		hiveConf = new HiveConfiguration(job);
		this.fieldSeparator = hiveConf.getOutputHiveFileFieldSplitChars();
		this.rowSeparator = hiveConf.getOutputHiveFileRowsSplitChars();
		this.fieldNames = hiveConf.getOutputFieldNames();
		boolean isCompressed = hiveConf.getOutputHiveCompress();
		this.orderOutputPath = hiveConf.getOutputTargetFilePath();
		this.partitionField = hiveConf.getOutputHivePartitionField();
		this.orderOutputTmpPath = hiveConf.getOutputHiveOrderTempPath();
		this.tableName = hiveConf.getOutputHiveTableName();
		if (this.orderOutputTmpPath.endsWith("/") && this.orderOutputTmpPath.length() > 2) {
			this.orderOutputTmpPath = this.orderOutputTmpPath.substring(0, this.orderOutputTmpPath.length() - 1);
		}

		String taskId = hiveConf.getConf().get("mapred.tip.id"); // 获取任务id
		if (taskId != null && taskId.length() >= 4) {
			orderOutputTmpPath += "/";
			orderOutputTmpPath += taskId.substring(taskId.length() - 4, taskId.length());
		}

		this.orderOutputFileNamePrefix = hiveConf.getOutputHiveOrderFileNamePrefix();
		this.orderWriteBufferSize = hiveConf.getOutputHiveOrderWriteBufferSize();
		this.orderOutputFileMaxCount = hiveConf.getOutputHiveOrderFileMaxCount();
		this.lstPartitionOutOS = new HashMap<String, RCFile.Writer>();
		this.lstPartitionFilePath = new HashMap<String, String>();
		this.lstPartitionFileCount = new HashMap<String, Long>();

		// 若分区，排除到分区字段，顺序不变
		if (null != this.partitionField && this.partitionField.length > 0 &&
				this.fieldNames.length >= this.partitionField.length) {
			String[] tmpFieldNames = new String[this.fieldNames.length - this.partitionField.length];
			int index = 0;
			boolean flag = false;
			for (int i = 0; i < this.fieldNames.length; i++) {
				flag = false;
				for (int j = 0; j < this.partitionField.length; j++) {
					if (this.fieldNames[i].equals(this.partitionField[j])) {
						flag = true;
						break;
					}
				}

				if (!flag) {
					tmpFieldNames[index] = this.fieldNames[i];
					index++;
				}
			}

			MRLog.consoleDebug(LOG, "partition write!");
			this.fieldNames = tmpFieldNames;
		} else {
			// 无需压缩
			if (!isCompressed) {
				// Path file = FileOutputFormat.getTaskOutputPath(context);
				// String name =
				// context.getConfiguration().get(HiveConfiguration.OUTPUT_HIVE_ORDER_FILENAME,
				// "PART");
				String extension = ".data";
				Path file = null;
				// file =
				// FileOutputFormat.getPathForWorkFile((TaskInputOutputContext)
				// context, name, extension);
				file = inputOutputFormat.getDefaultWorkFile(context, extension);
				// 输出目录
				// file = new Path(FileOutputFormat.getOutputPath(context),
				// FileOutputFormat.getUniqueFile(context, name,
				// extension));
				FileSystem fs = file.getFileSystem(job);
				this.out = new RCFile.Writer(fs, job, file, context, null);
			} else {
				// 需要压缩
				// 获取压缩标识
				String compresseCodec = hiveConf.getOutputHiveCompressCodec();
				CompressionCodec codec = HDFSUtils.getCompressCodec(compresseCodec, job);
				// build the filename including the extension
				Path file = null;
				file = inputOutputFormat.getDefaultWorkFile(context, codec.getDefaultExtension());
				FileSystem fs = file.getFileSystem(job);
				this.out = new RCFile.Writer(fs, job, file, context, codec);
			}
		}
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		// public void close(Reporter reporter) throws IOException {
		for (String key : this.lstPartitionFilePath.keySet()) {
			String realPath = lstPartitionFilePath.get(key);
			RCFile.Writer outos = this.lstPartitionOutOS.remove(realPath);
			if (outos == null) {
				continue;
			}
			if (null != outos) {
				outos.close();
			}
			this.moveFile(key, realPath);
		}

		// 删除临时目录下创建分区目录
		HDFSUtils.deleteDir(this.job, orderOutputTmpPath);

		if (this.lstPartitionOutOS.size() > 0) {
			MRLog.consoleWarn(LOG, "output stream close warn");
		}
		this.out.close();
		LOG.info("Total: insert " + rowCount + " rows");
		// MRLog.consoleDebug(LOG, "Total: insert " + rowCount + " rows");
	}

	@Override
	public void write(K key, V value) throws IOException, InterruptedException {
		boolean nullKey = key == null || key instanceof NullWritable;
		boolean nullValue = value == null || value instanceof NullWritable;
		if (nullKey && nullValue) {
			return;
		}

		if (!nullKey && key instanceof DBRecord) {
			if (null == this.partitionField || this.partitionField.length <= 0) {
				((DBRecord) key).write(this.out, this.fieldNames, this.fieldSeparator, this.rowSeparator);
				this.isNotPartitionsFistRow = false;
			} else {// 分区输出
					// 文件的第一行标示符
				boolean isFirstRow = false;
				if (null == key || !(key instanceof DBRecord) || null == value || !(value instanceof DBRecord)) {
					LOG.error("hive write key is null");
					throw new IOException("write key is null");
				}
				String partPath = this.getFilePath(key);
				String realFilePath = this.lstPartitionFilePath.get(partPath);
				RCFile.Writer filterFSOStream = this.lstPartitionOutOS.get(realFilePath);
				long count = 0;
				if (filterFSOStream == null) {
					System.out.println("1 filterFSOStream=" + filterFSOStream);
					this.initOutputStream(realFilePath);
					System.out.println("2 filterFSOStream=" + this.lstPartitionOutOS.get(realFilePath) + " path=" +
							realFilePath);
					filterFSOStream = this.lstPartitionOutOS.get(realFilePath);
					isFirstRow = true;
				} else {
					if (this.orderOutputFileMaxCount != -1) {
						count = this.lstPartitionFileCount.get(realFilePath);
						if (count >= this.orderOutputFileMaxCount) {
							// 移除之前的文件
							this.closeOutputStream(filterFSOStream);
							this.lstPartitionFilePath.remove(partPath);
							this.lstPartitionOutOS.remove(realFilePath);
							this.lstPartitionFileCount.remove(realFilePath);
							this.moveFile(partPath, this.lstPartitionFilePath.get(partPath));

							// 创建新的文件
							partPath = this.getFilePath(key);
							realFilePath = this.lstPartitionFilePath.get(partPath);
							this.initOutputStream(realFilePath);
							filterFSOStream = this.lstPartitionOutOS.get(realFilePath);
							count = 0;
							System.out.println("3 filterFSOStream=" + this.lstPartitionOutOS.get(realFilePath) +
									" path=" + realFilePath);
						}
					}
				}

				((DBRecord) value).write(filterFSOStream, this.fieldNames, this.fieldSeparator, this.rowSeparator);
				this.lstPartitionFileCount.put(realFilePath, ++count);
			}

			this.rowCount++;
			if (rowCount % 200000 == 0) {
				LOG.info("insert " + rowCount + " rows");
				// MRLog.consoleDebug(LOG, "insert " + rowCount + " rows");
			}
		}
	}

	private void closeOutputStream(RCFile.Writer filterFSOStream) throws IOException {
		filterFSOStream.close();
	}

	private void initOutputStream(String realFilePath) throws IOException {
		HiveConfiguration hiveConf = new HiveConfiguration(job);
		boolean isCompressed = hiveConf.getOutputHiveCompress();

		// 无需压缩
		if (!isCompressed) {
			Path file = new Path(realFilePath);
			FileSystem fs = file.getFileSystem(job);
			RCFile.Writer out = new RCFile.Writer(fs, job, file, null, null);
			this.lstPartitionOutOS.put(realFilePath, out);
			this.lstPartitionFileCount.put(realFilePath, 0l);
			return;
		}

		// 需要压缩
		// 获取压缩标识
		String compresseCodec = hiveConf.getOutputHiveCompressCodec();
		CompressionCodec codec = HDFSUtils.getCompressCodec(compresseCodec, job);

		Path file = new Path(realFilePath);
		FileSystem fs = file.getFileSystem(job);
		RCFile.Writer out = new RCFile.Writer(fs, job, file, null, codec);
		this.lstPartitionOutOS.put(realFilePath, out);
		this.lstPartitionFileCount.put(realFilePath, 0l);
	}

	private String getFilePath(K key) {
		String pathName = "";
		String partPath = "";
		String[] partition = new String[this.partitionField.length];
		for (int i = 0; i < this.partitionField.length; i++) {
			Object obj = ((DBRecord) key).getData(this.partitionField[i]);
			if (obj != null) {
				pathName += "/" + this.partitionField[i] + "=" + obj.toString();
				if (i == 0) {
					partPath += this.partitionField[i] + "=" + obj.toString();
				} else {
					partPath += "/" + this.partitionField[i] + "=" + obj.toString();
				}
				partition[i] = this.partitionField[i] + "=" + obj.toString();
			}
		}

		if (this.lstPartitionFilePath.containsKey(partPath)) {
			return partPath;
		}

		// 创建分区
		this.createHivePartion(partition);

		ticket++;
		pathName += "/" + this.orderOutputFileNamePrefix + System.currentTimeMillis() + "_" + ticket;
		HiveConfiguration hiveConf = new HiveConfiguration(job);
		boolean isCompressed = hiveConf.getOutputHiveCompress();
		// 需压缩
		if (isCompressed) {
			// 获取压缩标识
			String compresseCodec = hiveConf.getOutputHiveCompressCodec();
			CompressionCodec codec = HDFSUtils.getCompressCodec(compresseCodec, job);

			// build the filename including the extension
			pathName += codec.getDefaultExtension();
		}
		this.lstPartitionFilePath.put(partPath, this.orderOutputTmpPath + pathName);
		return partPath;
	}

	private void createHivePartion(String[] partition) {
		// 创建分区
		StringBuilder sql = new StringBuilder();
		sql.append("ALTER TABLE ");
		sql.append(tableName);
		sql.append(" ADD IF NOT EXISTS PARTITION(");
		for (int i = 0; i < partition.length; i++) {
			if (partition != null && partition.toString().length() > 0) {
				sql.append(partition[i]);
			}

			if (i != (partition.length - 1)) {
				sql.append(",");
			}
		}
		sql.append(")");
		System.out.println("partion sql:" + sql.toString());
		try {
			HiveOutputFormat.executeDDLHQL(this.hiveConf, sql.toString());
		} catch (SQLException e) {
			MRLog.warn(LOG, "create hive partion error, msg:" + StringUtil.stringifyException(e));
		}
	}

	private void moveFile(String key, String realPath) throws IOException {
		Path srcPath = new Path(realPath);
		// System.out.println("this.orderOutputPath="+this.orderOutputPath);
		// System.out.println("key="+key);
		// System.out.println("srcPath.getName()="+srcPath.getName());
		// System.out.println("dstPath="+realPath +" dstPath" +
		// (this.orderOutputPath + key +"/"+ srcPath.getName()));
		Path dstPaths = new Path(this.orderOutputPath + key);
		if (!srcPath.getFileSystem(job).exists(dstPaths)) {
			System.out.println(srcPath.getFileSystem(job).mkdirs(dstPaths));
		}
		Path dstPath = new Path(this.orderOutputPath + key + "/" + srcPath.getName());
		FileSystem srcFs = srcPath.getFileSystem(this.job);
		srcFs.rename(srcPath, dstPath);
	}
}
