/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ery.hadoop.mrddx.file;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.zip.ZipInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.tools.tar.TarInputStream;

import com.ery.hadoop.mrddx.MRConfiguration;

/**
 * Treats keys as offset in file and value as line.
 */
public class LineRecordReader extends RecordReader<LongWritable, Text> {
	private static final Log LOG = LogFactory.getLog(LineRecordReader.class.getName());

	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	private long totalend;
	private long finishLen;
	private LineReader in;
	int maxLineLength;
	private Seekable filePosition;
	private CompressionCodec codec;
	private Decompressor decompressor;
	private FileSplit split;
	private Configuration job;
	private LongWritable key = null;
	private Text value = null;
	int perFileSkipRowNum;
	private String fileEncodeing;

	/**
	 * A class that provides a line reader from an input stream.
	 * 
	 * @deprecated Use {@link org.apache.hadoop.util.LineReader} instead.
	 */
	@Deprecated
	public static class LineReader extends LineReaders {

		LineReader(InputStream in, int skipNum) {
			super(in, LineReaders.DEFAULT_BUFFER_SIZE, skipNum);
		}

		LineReader(InputStream in, int bufferSize, int skipNum) {
			super(in, bufferSize, skipNum);
		}

		public LineReader(InputStream in, Configuration conf) throws IOException {
			super(in, conf);
		}
	}

	public LineRecordReader(Configuration job, FileSplit split) throws IOException {
		this.perFileSkipRowNum = job.getInt(FileConfiguration.INPUT_FILE_SKIP_ROWNUM, 0);
		this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
		this.job = job;
		this.split = split;
		for (long l : split.getLengths()) {
			totalend += l;
		}
		this.fileEncodeing = job.get(MRConfiguration.FILE_CONTENT_ENCODING,
				MRConfiguration.FILE_CONTENT_ENCODING_DEFAULT).toLowerCase();
		if (this.fileEncodeing.equals("")) {
			this.fileEncodeing = "utf-8";
		}
		this.split.setFileIndex(0);
		this.openFile();
	}

	private boolean isCompressedInput() {
		return (codec != null);
	}

	private int maxBytesToConsume(long pos) {
		return isCompressedInput() ? Integer.MAX_VALUE : (int) Math.min(Integer.MAX_VALUE, end - pos);
	}

	private long getFilePosition() throws IOException {
		long retVal;
		if (isCompressedInput() && null != filePosition) {
			retVal = filePosition.getPos();
		} else {
			retVal = pos;
		}
		return retVal;
	}

	public LineRecordReader(InputStream in, long offset, long endOffset, int maxLineLength, int skipNum) {
		this.maxLineLength = maxLineLength;
		this.perFileSkipRowNum = skipNum;
		this.in = new LineReader(in, perFileSkipRowNum);
		this.start = offset;
		this.pos = offset;
		this.end = endOffset;
		this.filePosition = null;
		this.fileEncodeing = job.get(MRConfiguration.FILE_CONTENT_ENCODING,
				MRConfiguration.FILE_CONTENT_ENCODING_DEFAULT).toLowerCase();
		if (this.fileEncodeing.equals("")) {
			this.fileEncodeing = "utf-8";
		}
	}

	public LineRecordReader(InputStream in, long offset, long endOffset, Configuration job) throws IOException {
		this.job = job;
		this.perFileSkipRowNum = job.getInt(FileConfiguration.INPUT_FILE_SKIP_ROWNUM, 0);
		this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
		this.in = new LineReader(in, job);
		this.start = offset;
		this.pos = offset;
		this.end = endOffset;
		this.filePosition = null;
	}

	public LongWritable createKey() {
		return new LongWritable();
	}

	public Text createValue() {
		return new Text();
	}

	/** Read a line. */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// public synchronized boolean next(LongWritable key, Text value) throws
		// IOException {
		if (key == null) {
			key = new LongWritable();
		}
		if (value == null) {
			value = createValue();
		}
		// We always read one extra line, which lies outside the upper
		// split limit i.e. (end - 1)
		do {
			while (getFilePosition() <= end) {
				key.set(pos);
				int newSize = in.readLine(value, maxLineLength, Math.max(maxBytesToConsume(pos), maxLineLength));
				if (newSize <= 0) {
					if (getNextFile()) {
						continue;
						// return nextKeyValue();
					} else {
						return false;
					}
				}
				pos += newSize;
				if (newSize < maxLineLength) {
					return true;
				}
				// line too long. try again
				LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
			}
		} while (getNextFile());
		return false;
	}

	public String getFilePath() {
		return split.getPath().getName();
	}

	public String getContextFileName() {
		if (in.getInputStream() instanceof TarInputStream || in.getInputStream() instanceof ZipInputStream) {
			return in.getContextFileName();
		}
		return split.getPath().getName();
	}

	void openFile() throws IOException {
		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();
		LOG.info("split.getFileIndex=" + split.getFileIndex() + ",file.path=" + file.toString() + " fileEncodeing=" +
				fileEncodeing + " " + split.getStart() + ":" + split.getLength());
		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());

		compressionCodecs = new CompressionCodecFactory(job);
		codec = compressionCodecs.getCodec(file);
		if (file.getName().endsWith(".zip")) {
			LOG.info("use ZipInputStream read file " + split.getPath());
			ZipInputStream zin = new ZipInputStream(fileIn, Charset.forName(fileEncodeing));
			in = new LineReader(zin, job);
			filePosition = fileIn;
			codec = new GzipCodec();
			return;
		}
		if (isCompressedInput()) {
			decompressor = CodecPool.getDecompressor(codec);
			if (codec instanceof SplittableCompressionCodec) {
				final SplitCompressionInputStream cIn = ((SplittableCompressionCodec) codec).createInputStream(fileIn,
						decompressor, start, end, SplittableCompressionCodec.READ_MODE.BYBLOCK);
				// 如果是tar.gz，使用TarInputStream
				// new TarInputStream(codec.createInputStream(fileIn,
				// decompressor)
				String filename = file.getName();
				if (filename.endsWith(".tar.gz")) {
					in = new LineReader(new TarInputStream(cIn), job);
				} else {
					in = new LineReader(cIn, job);
				}
				start = cIn.getAdjustedStart();
				end = cIn.getAdjustedEnd();
				filePosition = cIn; // take pos from compressed stream
			} else {
				String filename = file.getName();
				if (filename.endsWith(".tar.gz") || filename.endsWith(".tar")) {
					in = new LineReader(new TarInputStream(codec.createInputStream(fileIn, decompressor)), job);
				} else {
					in = new LineReader(codec.createInputStream(fileIn, decompressor), job);
				}
				filePosition = fileIn;
			}
		} else {
			fileIn.seek(start);
			String filename = file.getName();
			if (filename.endsWith(".tar")) {
				in = new LineReader(new TarInputStream(fileIn), job);
			} else {
				in = new LineReader(fileIn, job);
			}

			filePosition = fileIn;
		}
		// If this is not the first split, we always throw away first record
		// because we always (except the last split) read one extra line in
		// next() method.
		if (start != 0) {
			start += in.readLine(new Text(), 0, maxBytesToConsume(start));
		}
		this.pos = start;
	}

	private boolean getNextFile() throws IOException {
		if (split.getFileIndex() + 1 >= split.getFileMaxIndex()) {// 已经没有文件了
			LOG.info("split.getFileIndex=" + split.getFileIndex() + ",totalFiles=" + split.getFileMaxIndex());
			return false;
		}

		finishLen += split.getLength();
		split.setFileIndex(split.getFileIndex() + 1);
		this.openFile();
		return true;
	}

	/**
	 * Get the progress within the split
	 */
	public float getProgress() throws IOException {
		if (start == totalend) {
			return 0.0f;
		} else {
			return Math.min(1.0f, ((getFilePosition() - start) + finishLen) / (float) totalend);
		}
	}

	public synchronized long getPos() throws IOException {
		return pos;
	}

	public synchronized void close() throws IOException {
		try {
			if (in != null) {
				in.close();
			}
		} finally {
			if (decompressor != null) {
				CodecPool.returnDecompressor(decompressor);
			}
		}
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return this.key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return this.value;
	}
}