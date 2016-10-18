package com.ery.hadoop.mrddx.remote;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.ery.hadoop.mrddx.IHandleFormat;
import com.ery.hadoop.mrddx.db.NullOutputCommitter;

public class FTPOutputFormat<K extends Text, V> extends NullOutputFormat<K, NullWritable> implements IHandleFormat {

	@Override
	public RecordWriter<K, NullWritable> getRecordWriter(TaskAttemptContext context) {
		return null;
	}

	@Override
	public void checkOutputSpecs(JobContext job) {
		// 检查FTP是否可访问等
	}

	@Override
	public void handle(Job conf) throws Exception {

	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
		return new NullOutputCommitter();
	}
}