package com.ery.hadoop.mrddx.file;

import org.apache.hadoop.conf.Configuration;

import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.util.HDFSUtils.CompressCodec;

/**
 * 与文件(输入和输出)有关的配置信息
 * 



 * @createDate 2013-2-21
 * @version v1.0
 */
public class FileConfiguration extends MRConfiguration {
	// 输入标示符
	public static final int FLAG_FILE_INPUT = 0;

	// 输出标示符
	public static final int FLAG_FILE_OUTPUT = 1;

	/**
	 * 文件(输入)
	 */
	// 文件数据源参数
	public static final String INPUT_FILE_FS_DEFAULT_NAME = "input.mr.mapred.fs.default.name";
	// 文件的列
	public static final String INPUT_FILE_FIELD_SPLIT_CHARS = "input.mr.mapred.file.field.split.chars";
	public static final String INPUT_FILE_ROWS_SPLIT_CHARS = "input.mr.mapred.file.rows.split.chars";
	// 是否设置文件过滤
	public static final String INPUT_FILE_IS_FILTER = "input.mr.mapred.file.is.filter";
	// 文件过滤正则表达式
	public static final String INPUT_FILE_REGEXPATHFILTER = "input.mr.mapred.file.regexpathfilter.regex";
	// 文件过滤类型(true：处理匹配正则表达式的文件，false:不处理匹配正则表达式的文件)
	public static final String INPUT_FILE_REGEXPATHFILTER_TYPE = "input.mr.mapred.file.regexpathfilter.regex.type";
	// 对单个文件而言，设置跳过前面的行，不进行处理，注：以\n为分割的行
	public static final String INPUT_FILE_SKIP_ROWNUM = "input.mr.mapred.file.skip.rownum";

	// 处理文件日志的类型(force:1, joblogid:2, joblogtime:3, jobfail:4);
	public static final String INPUT_FILE_JOBLOG_TYPE = "input.mr.mapred.file.joblog.type";
	// 强制处理标示符(则不需要根据jobid+文件id查询最大的日志id，直接新增文件处理日志)
	public static final String INPUT_FILE_FORCE_DO = "input.mr.mapred.file.force.do";
	// 处理文件的日志ID
	public static final String INPUT_FILE_JOBLOGID_DO = "input.mr.mapred.file.joblogid.do";
	// 处理文件的日志时间段标示符,格式(20121203,20121204)
	public static final String INPUT_FILE_JOBLOGTIME_DO = "input.mr.mapred.file.joblogtime.do";
	// 处理任务ID下的所有失败文件标示符
	public static final String INPUT_FILE_JOBFAIL = "input.mr.mapred.file.jobfail";

	/**
	 * 文件(输出)
	 */
	// 文件数据源参数
	public static final String OUTPUT_FILE_FS_DEFAULT_NAME = "output.mr.mapred.fs.default.name";
	// 输出列分隔符
	public static final String OUTPUT_FILE_FIELD_SPLIT_CHARS = "output.mr.mapred.file.field.split.chars";
	// 输出行分隔符
	public static final String OUTPUT_FILE_ROWS_SPLIT_CHARS = "output.mr.mapred.file.rows.split.chars";
	// 输出文件的是否压缩
	public static final String OUTPUT_FILE_COMPRESS = "output.mr.mapred.file.iscompress";
	// 输出文件压缩方式
	public static final String OUTPUT_FILE_COMPRESSCODEC = "output.mr.mapred.file.compressCodec";
	// 输出文件压缩类型
	public static final String OUTPUT_FILE_COMPRESSCODEC_TYPE = "output.mr.mapred.file.compressCodec.type";
	// move文件的目录
	public static final String OUTPUT_FILE_TARGET_PATH = "output.mr.mapred.file.target.Path";

	public FileConfiguration(Configuration job) {
		super(job);
		this.conf = job;
	}

	/**
	 * 默认构造方法
	 * 
	 * @param job
	 *            job对象
	 * @param flag
	 *            输入输出标示符
	 */
	public FileConfiguration(Configuration job, int flag) {
		super(job);
		this.conf = job;
		switch (flag) {
		case 0:
			this.conf.set(MRConfiguration.FS_DEFAULT_NAME, this.getInputFileFSDefaultName());
			break;
		case 1:
			this.conf.set(MRConfiguration.FS_DEFAULT_NAME, this.getOutputFileFSDefaultName());
			break;
		default:
			break;
		}
	}

	public String getInputFileFSDefaultName() {
		return conf.get(INPUT_FILE_FS_DEFAULT_NAME, "");
	}

	public void setInputFileFSDefaultName(String fsDefaultName) {
		conf.set(INPUT_FILE_FS_DEFAULT_NAME, fsDefaultName);
	}

	public String getInputFileFieldSplitChars() {
		return conf.get(INPUT_FILE_FIELD_SPLIT_CHARS, ",");
	}

	public void setInputFileFieldSplitChars(String splitChars) {
		conf.set(INPUT_FILE_FIELD_SPLIT_CHARS, splitChars);
	}

	public String getInputFileRowsSplitChars() {
		return conf.get(INPUT_FILE_ROWS_SPLIT_CHARS, "\n");
	}

	/**
	 * @param splitChars
	 *            必需以 \n结尾
	 */
	public void setInputFileRowsSplitChars(String splitChars) throws Exception {
		conf.set(INPUT_FILE_ROWS_SPLIT_CHARS, splitChars);
	}

	public boolean getInputFileIsFilter() {
		return conf.getBoolean(INPUT_FILE_IS_FILTER, false);
	}

	public void setInputFileIsFilter(boolean isFilter) {
		conf.setBoolean(INPUT_FILE_IS_FILTER, isFilter);
	}

	public int getInputFileSkipRowNum() {
		return conf.getInt(INPUT_FILE_SKIP_ROWNUM, -1);
	}

	public void setInputFileSkipRowNum(int skiprownum) {
		conf.setInt(INPUT_FILE_SKIP_ROWNUM, skiprownum);
	}

	public int getInputFileJobLogType() {
		return conf.getInt(INPUT_FILE_JOBLOG_TYPE, -1);
	}

	public boolean getInputFileForceDo() {
		return conf.getBoolean(INPUT_FILE_FORCE_DO, false);
	}

	public long getInputFileJobLogId() {
		return conf.getLong(INPUT_FILE_JOBLOGID_DO, -1);
	}

	public String[] getInputFileJobLogTime() {
		return conf.getStrings(INPUT_FILE_JOBLOGTIME_DO, new String[0]);
	}

	public boolean getInputFileJobFail() {
		return conf.getBoolean(INPUT_FILE_JOBFAIL, false);
	}

	public String getOutputFileFSDefaultName() {
		return conf.get(OUTPUT_FILE_FS_DEFAULT_NAME, "");
	}

	public void setOutputFileFSDefaultName(String fsDefaultName) {
		conf.set(OUTPUT_FILE_FS_DEFAULT_NAME, fsDefaultName);
	}

	/**
	 * 设置文件是否需要被压缩
	 * 
	 * @param pTargetCompress
	 *            true：压缩
	 */
	public void setOutputFileCompress(boolean pTargetCompress) {
		conf.setBoolean(OUTPUT_FILE_COMPRESS, pTargetCompress);
	}

	public boolean getOutputFileCompress() {
		return conf.getBoolean(OUTPUT_FILE_COMPRESS, false);
	}

	/**
	 * 设置输出文件的压缩方式 只有当mapred.file.output.file.iscompress为true才有效
	 * 
	 * @param pTargetCompressCodec
	 *            (参照：HDFSUtils.CompressCodec)
	 */
	public void setOutputFileCompressCodec(String pTargetCompressCodec) {
		conf.set(OUTPUT_FILE_COMPRESSCODEC, pTargetCompressCodec);
	}

	public String getOutputFileCompressCodec() {
		return conf.get(OUTPUT_FILE_COMPRESSCODEC, CompressCodec.DefaultCodec.toString());
	}

	/**
	 * 设置输出文件的压缩方式 只有当mapred.file.output.file.iscompress为true才有效
	 * 
	 * @param pTargetCompressCodec
	 *            (参照：HDFSUtils.CompressCodec)
	 */
	public void setOutputFileCompressCodecType(String pTargetCompressCodecType) {
		conf.set(OUTPUT_FILE_COMPRESSCODEC_TYPE, pTargetCompressCodecType);
	}

	public String getOutputFileCompressCodecType() {
		return conf.get(OUTPUT_FILE_COMPRESSCODEC_TYPE, super.getOutputCompressionType().toString());
	}

	public String getOutputFileFieldSplitChars() {
		return conf.get(OUTPUT_FILE_FIELD_SPLIT_CHARS, ",");
	}

	public void setOutputFileFieldSplitChars(String splitChars) {
		conf.set(OUTPUT_FILE_FIELD_SPLIT_CHARS, splitChars);
	}

	public String getOutputFileRowsSplitChars() {
		return conf.get(OUTPUT_FILE_ROWS_SPLIT_CHARS, "\n");
	}

	/**
	 * @param splitChars
	 *            必需以 \n结尾
	 */
	public void setOutputFileRowsSplitChars(String splitChars) throws Exception {
		if (splitChars.endsWith("\n") == false)
			throw new Exception("文件中行分隔符必需以 \\n结尾");
		conf.set(OUTPUT_FILE_ROWS_SPLIT_CHARS, splitChars);
	}

	public String getOutputTargetFilePath() {
		return conf.get(OUTPUT_FILE_TARGET_PATH, "");
	}

	public void setOutputTargetFilePath(String path) {
		conf.set(OUTPUT_FILE_TARGET_PATH, path);
	}
}
