package com.ery.hadoop.mrddx.file;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.ery.hadoop.mrddx.IHandleFormat;
import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.log.MRLog;

/**
 * Sequence文件输入格式
 * 



 * @createDate 2013-1-10
 * @version v1.0
 */
public class SequenceFileInputFormat extends TextInputFormat implements IHandleFormat {
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {

		// public RecordReader getRecordReader(InputSplit split, JobConf job,
		// Reporter reporter) throws IOException {
		// 日志
		long logId = context.getConfiguration().getLong(MRConfiguration.INTERNAL_JOB_LOG_ID, -1);
		MRLog.info(LOG, logId, "Get sequence recordReader");
		context.setStatus(split.toString());
		try {
			return new SequenceFileRecordReader(context.getConfiguration(), (FileSplit) split);
		} catch (Exception e) {
			return null;
		}
	}

	@Override
	protected List<FileStatus> listStatus(JobContext job) throws IOException {
		List<FileStatus> fileStatus = super.listStatus(job);
		// 若为SEQ文件，需要验证文件是否正确
		TextParse textParse = new TextParse();
		if (!textParse.validateInputSEQ(job.getConfiguration(), fileStatus)) {
			String meg = "源文件列表为空或者不是SEQ文件";
			MRLog.error(LOG, meg);
			throw new IOException(meg);
		}
		return fileStatus;
	}

	@Override
	public void handle(Job conf) throws Exception {
		super.handle(conf);
		conf.setInputFormatClass(SequenceFileInputFormat.class);
	}
}
