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

import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.RCFileMapper;
import com.ery.hadoop.mrddx.log.MRLog;

/**
 * RCFile输入格式
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-2-21
 * @version v1.0
 */
public class RCFileInputFormat extends TextInputFormat {
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
		// 日志
		long logId = context.getConfiguration().getLong(MRConfiguration.INTERNAL_JOB_LOG_ID, -1);
		MRLog.info(LOG, logId, "Get rc recordReader");
		context.setStatus(split.toString());
		try {
			return new RCFileRecordReader(context.getConfiguration(), (FileSplit) split);
		} catch (Exception e) {
			return null;
		}
	}

	@Override
	protected List<FileStatus> listStatus(JobContext context) throws IOException {
		List<FileStatus> fileStatus = super.listStatus(context);
		// 若为RC文件，需要验证文件是否正确
		RCFileParse rcfileParse = new RCFileParse();
		if (!rcfileParse.validateInputRCFile(context.getConfiguration(), fileStatus)) {
			String meg = "源文件列表为空或者不是RC文件";
			MRLog.error(LOG, meg);
			throw new IOException(meg);
		}

		return fileStatus;
	}

	@Override
	public void handle(Job conf) throws Exception {
		super.handle(conf);
		conf.setInputFormatClass(RCFileInputFormat.class);
		conf.setMapperClass(RCFileMapper.class);
	}
}
