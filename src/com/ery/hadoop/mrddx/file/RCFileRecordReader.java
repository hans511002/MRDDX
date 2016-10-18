package com.ery.hadoop.mrddx.file;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFile.Reader;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 读取RCFile格式文件
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-1-11
 * @version v1.0
 * @param <K>
 * @param <V>
 */
public class RCFileRecordReader<K extends LongWritable, V extends BytesRefArrayWritable> extends
		RecordReader<LongWritable, BytesRefArrayWritable> {
	// 读取对象
	private final Reader in;

	// 开始索引
	private final long start;

	// 结束索引
	private final long end;

	// 是否有下一条
	private boolean more = true;

	// 配置对象
	protected Configuration conf;
	LongWritable key = null;
	BytesRefArrayWritable value = null;

	/**
	 * 构造方法
	 * 
	 * @param conf 配置对象
	 * @param split 文件拆分对象
	 * @throws IOException IO异常
	 */
	public RCFileRecordReader(Configuration conf, FileSplit split) throws IOException {
		Path path = split.getPath();
		FileSystem fs = path.getFileSystem(conf);
		this.in = new RCFile.Reader(fs, path, conf);
		this.end = split.getStart() + split.getLength();
		this.conf = conf;

		if (split.getStart() > this.in.getPosition()) {
			this.in.sync(split.getStart()); // sync to start
		}

		this.start = this.in.getPosition();
		this.more = this.start < this.end;
	}

	public Class<?> getKeyClass() {
		return LongWritable.class;
	}

	public Class<?> getValueClass() {
		return BytesRefArrayWritable.class;
	}

	public LongWritable createKey() {
		return (LongWritable) ReflectionUtils.newInstance(getKeyClass(), conf);
	}

	public BytesRefArrayWritable createValue() {
		return (BytesRefArrayWritable) ReflectionUtils.newInstance(getValueClass(), this.conf);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (key == null)
			key = this.createKey();
		if (value == null) {
			value = this.createValue();
		}
		return next(key, value);
	}

	public boolean next(LongWritable key, BytesRefArrayWritable value) throws IOException {
		more = next(key);
		if (more) {
			in.getCurrentRow(value);
		}
		return more;
	}

	protected boolean next(LongWritable key) throws IOException {
		if (!this.more) {
			return false;
		}

		this.more = this.in.next(key);
		if (!this.more) {
			return false;
		}

		long lastSeenSyncPos = this.in.lastSeenSyncPos();
		if (lastSeenSyncPos >= this.end) {
			this.more = false;
			return this.more;
		}
		return this.more;
	}

	/**
	 * Return the progress within the input split.
	 * 
	 * @return 0.0 to 1.0 of the input byte range
	 */
	public float getProgress() throws IOException {
		if (end == start) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (in.getPosition() - start) / (float) (end - start));
		}
	}

	public long getPos() throws IOException {
		return in.getPosition();
	}

	protected void seek(long pos) throws IOException {
		in.seek(pos);
	}

	public void sync(long pos) throws IOException {
		in.sync(pos);
	}

	public void resetBuffer() {
		in.resetBuffer();
	}

	public long getStart() {
		return start;
	}

	public void close() throws IOException {
		in.close();
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
	public BytesRefArrayWritable getCurrentValue() throws IOException, InterruptedException {
		return this.value;
	}
}