package com.ery.hadoop.mrddx.remote;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import com.ery.hadoop.mrddx.remote.FTPInputFormat.FTPInputSplit;

public class FTPRecordReader<T extends FTPWritable> extends RecordReader<LongWritable, T> {
	public static final String RECORD = "record";

	private Class<T> inputClass;
	private Configuration conf;
	private long pos = 0;
	private LongWritable key = null;
	private T value = null;
	private String[] fieldName;
	private int currentIndex;
	private SplitPathPO[] pathValues;
	FTPInputSplit split;

	// public FTPRecordReader() {
	// }

	public FTPRecordReader(FTPInputSplit split, Class<T> clazz, Configuration conf) {
		this.inputClass = clazz;
		this.conf = conf;
		this.fieldName = new String[] { RECORD };
		if (null != split) {
			this.pathValues = split.getArraySplitPath();
		}
		this.split = split;
	}

	public boolean next(LongWritable key, T value) throws IOException, InterruptedException {
		this.key = key;
		this.value = value;
		return this.nextKeyValue();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (this.key == null) {
			this.key = new LongWritable();
		}
		if (this.value == null) {
			this.value = this.createValue();
		}
		if (null == this.pathValues) {
			return false;
		}
		if (this.currentIndex + 1 > this.pathValues.length) {
			return false;
		}
		this.value.readFields(this.pathValues[this.currentIndex], this.fieldName);
		this.split.currentIndex = this.currentIndex;
		this.currentIndex++;
		return true;
	}

	public LongWritable createKey() {
		return new LongWritable(0);
	}

	public T createValue() {
		return ReflectionUtils.newInstance(this.inputClass, this.conf);
	}

	public long getPos() throws IOException {
		return this.pos;
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public float getProgress() throws IOException {
		if (null == this.pathValues || 0 == this.pathValues.length) {
			return 1f;
		}

		return (float) this.currentIndex / this.pathValues.length;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return this.key;
	}

	@Override
	public T getCurrentValue() throws IOException, InterruptedException {
		return this.value;
	}
}