package com.ery.hadoop.mrddx.file;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 读取Sequence文件
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-1-10
 * @version v1.0
 * @param <K>
 * @param <V>
 */
public class SequenceFileRecordReader<K, V> extends RecordReader<K, V> {
	K key = null;
	V value = null;
	// 读取对象
	private SequenceFile.Reader in;

	// 开始索引
	private long start;

	// 结束索引
	private long end;

	// 是否有下一条
	private boolean more = true;

	// 配置对象
	protected Configuration conf;

	/**
	 * 构造方法
	 * 
	 * @param conf 配置对象
	 * @param split 拆分对象
	 * @throws IOException IO异常
	 */
	public SequenceFileRecordReader(Configuration conf, FileSplit split) throws IOException {
		Path path = split.getPath();
		FileSystem fs = path.getFileSystem(conf);
		this.in = new SequenceFile.Reader(fs, path, conf);
		this.end = split.getStart() + split.getLength();
		this.conf = conf;
		if (split.getStart() > this.in.getPosition()) {
			this.in.sync(split.getStart()); // sync to start
		}

		this.start = this.in.getPosition();
		this.more = this.start < this.end;
	}

	/**
	 * The class of key that must be passed to {@link #next(Object, Object)}..
	 */
	public Class<?> getKeyClass() {
		return this.in.getKeyClass();
	}

	/**
	 * The class of value that must be passed to {@link #next(Object, Object)}..
	 */
	public Class<?> getValueClass() {
		return this.in.getValueClass();
	}

	@SuppressWarnings("unchecked")
	public K createKey() {
		return (K) ReflectionUtils.newInstance(getKeyClass(), conf);
	}

	@SuppressWarnings("unchecked")
	public V createValue() {
		return (V) ReflectionUtils.newInstance(getValueClass(), this.conf);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (key == null) {
			key = this.createKey();
		}
		if (value == null) {
			value = this.createValue();
		}
		return next(key, value);
	}

	public synchronized boolean next(K key, V value) throws IOException {
		if (!this.more) {
			return false;
		}
		long pos = this.in.getPosition();
		boolean remaining = (this.in.next(key) != null);
		if (remaining) {
			this.in.getCurrentValue(value);
		}
		if (pos >= this.end && this.in.syncSeen()) {
			this.more = false;
		} else {
			this.more = remaining;
		}
		return this.more;
	}

	protected synchronized boolean next(K key) throws IOException {
		if (!this.more) {
			return false;
		}
		long pos = this.in.getPosition();
		boolean remaining = (this.in.next(key) != null);
		if (pos >= this.end && this.in.syncSeen()) {
			this.more = false;
		} else {
			this.more = remaining;
		}
		return this.more;
	}

	@Override
	public K getCurrentKey() throws IOException, InterruptedException {
		return this.key;
	}

	@Override
	public V getCurrentValue() throws IOException, InterruptedException {
		return this.value;
	}

	/**
	 * Return the progress within the input split
	 * 
	 * @return 0.0 to 1.0 of the input byte range
	 */
	public float getProgress() throws IOException {
		if (this.end == this.start) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (this.in.getPosition() - this.start) / (float) (this.end - this.start));
		}
	}

	public synchronized long getPos() throws IOException {
		return this.in.getPosition();
	}

	protected synchronized void seek(long pos) throws IOException {
		this.in.seek(pos);
	}

	public synchronized void close() throws IOException {
		this.in.close();
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub

	}

}