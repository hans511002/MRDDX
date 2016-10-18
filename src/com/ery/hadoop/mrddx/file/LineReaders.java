package com.ery.hadoop.mrddx.file;

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

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;

/**
 * A class that provides a line reader from an input stream. Depending on the
 * constructor used, lines will either be terminated by:
 * <ul>
 * <li>one of the following: '\n' (LF) , '\r' (CR), or '\r\n' (CR+LF).</li>
 * <li><em>or</em>, a custom byte sequence delimiter</li>
 * </ul>
 * In both cases, EOF also terminates an otherwise unterminated line.
 */
public class LineReaders {
	private static final Log LOG = LogFactory.getLog(LineReaders.class.getName());
	public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
	private int bufferSize = DEFAULT_BUFFER_SIZE;
	private InputStream in;
	private byte[] buffer;
	// the number of bytes of real data in the buffer
	private int bufferLength = 0;
	// the current position in the buffer
	private int bufferPosn = 0;

	private static final byte CR = '\r';
	private static final byte LF = '\n';

	// The line delimiter
	private final byte[] recordDelimiterBytes;

	private boolean isreadtar;
	protected int perFileSkipRowNum = 0;
	int curFileSkipNum = 0;

	/**
	 * Create a line reader that reads from the given stream using the default
	 * buffer-size (64k).
	 * 
	 * @param in
	 *            The input stream
	 * @throws IOException
	 */
	public LineReaders(InputStream in, int skipNum) {
		this(in, DEFAULT_BUFFER_SIZE, skipNum);
	}

	/**
	 * Create a line reader that reads from the given stream using the given
	 * buffer-size.
	 * 
	 * @param in
	 *            The input stream
	 * @param bufferSize
	 *            Size of the read buffer
	 * @throws IOException
	 */
	public LineReaders(InputStream in, int bufferSize, int skipNum) {
		this.in = in;
		this.bufferSize = bufferSize;
		this.perFileSkipRowNum = skipNum;
		this.buffer = new byte[this.bufferSize];
		this.recordDelimiterBytes = null;
	}

	/**
	 * Create a line reader that reads from the given stream using the
	 * <code>io.file.buffer.size</code> specified in the given
	 * <code>Configuration</code>.
	 * 
	 * @param in
	 *            input stream
	 * @param conf
	 *            configuration
	 * @throws IOException
	 */
	public LineReaders(InputStream in, Configuration conf) throws IOException {
		this(in, conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE), conf.getInt(
				FileConfiguration.INPUT_FILE_SKIP_ROWNUM, 0));
	}

	/**
	 * Create a line reader that reads from the given stream using the default
	 * buffer-size, and using a custom delimiter of array of bytes.
	 * 
	 * @param in
	 *            The input stream
	 * @param recordDelimiterBytes
	 *            The delimiter
	 */
	public LineReaders(InputStream in, byte[] recordDelimiterBytes, int skipNum) {
		this.in = in;
		this.bufferSize = DEFAULT_BUFFER_SIZE;
		this.buffer = new byte[this.bufferSize];
		this.perFileSkipRowNum = skipNum;
		this.recordDelimiterBytes = recordDelimiterBytes;
	}

	/**
	 * Create a line reader that reads from the given stream using the given
	 * buffer-size, and using a custom delimiter of array of bytes.
	 * 
	 * @param in
	 *            The input stream
	 * @param bufferSize
	 *            Size of the read buffer
	 * @param recordDelimiterBytes
	 *            The delimiter
	 * @throws IOException
	 */
	public LineReaders(InputStream in, int bufferSize, byte[] recordDelimiterBytes, int skipNum) {
		this.in = in;
		this.bufferSize = bufferSize;
		this.perFileSkipRowNum = skipNum;
		this.buffer = new byte[this.bufferSize];
		this.recordDelimiterBytes = recordDelimiterBytes;
	}

	/**
	 * Create a line reader that reads from the given stream using the
	 * <code>io.file.buffer.size</code> specified in the given
	 * <code>Configuration</code>, and using a custom delimiter of array of
	 * bytes.
	 * 
	 * @param in
	 *            input stream
	 * @param conf
	 *            configuration
	 * @param recordDelimiterBytes
	 *            The delimiter
	 * @throws IOException
	 */
	public LineReaders(InputStream in, Configuration conf, byte[] recordDelimiterBytes, int skipNum) throws IOException {
		this.in = in;
		this.bufferSize = conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);
		this.perFileSkipRowNum = skipNum;
		this.buffer = new byte[this.bufferSize];
		this.recordDelimiterBytes = recordDelimiterBytes;
	}

	/**
	 * Close the underlying stream.
	 * 
	 * @throws IOException
	 */
	public void close() throws IOException {
		in.close();
	}

	public String getComtype() {
		if (in instanceof TarInputStream) // 获取tar包的开头
			return "tar";
		if (in instanceof ZipInputStream)
			return "zip";
		return null;
	}

	public InputStream getInputStream() {
		return this.in;
	}

	String contextFileName = null;

	public String getContextFileName() {
		return contextFileName;
	}

	/**
	 * Read one line from the InputStream into the given Text.
	 * 
	 * @param str
	 *            the object to store the given line (without newline)
	 * @param maxLineLength
	 *            the maximum number of bytes to store into str; the rest of the
	 *            line is silently discarded.
	 * @param maxBytesToConsume
	 *            the maximum number of bytes to consume in this call. This is
	 *            only a hint, because if the line cross this threshold, we
	 *            allow it to happen. It can overshoot potentially by as much as
	 *            one buffer length.
	 * 
	 * @return the number of bytes read including the (longest) newline found.
	 * 
	 * @throws IOException
	 *             if the underlying stream throws
	 */
	public int readLine(Text str, int maxLineLength, int maxBytesToConsume) throws IOException {
		int readsize = -1;
		if (!isreadtar) {
			isreadtar = true;
			if (in instanceof TarInputStream) {// 获取tar包的开头
				TarEntry te = ((TarInputStream) in).getNextEntry();
				System.out.print("input stream is TarInputStream  getNextEntry:");
				while (te != null && te.isDirectory()) {
					LOG.info(" dir: " + te.getName());
					te = ((TarInputStream) in).getNextEntry();
				}
				if (te == null)
					return -1;
				contextFileName = te.getName();
				LOG.info(" file: " + contextFileName);
			} else if (in instanceof ZipInputStream) {
				System.out.print("input stream is ZipInputStream  getNextEntry:");
				ZipEntry zin = ((ZipInputStream) in).getNextEntry();
				while (zin != null && zin.isDirectory()) {
					LOG.info(" dir: " + zin.getName());
					zin = ((ZipInputStream) in).getNextEntry();
				}
				if (zin == null)
					return -1;
				contextFileName = zin.getName();
				LOG.info(" file: " + contextFileName);
			}
		}
		if (this.recordDelimiterBytes != null) {
			readsize = readCustomLine(str, maxLineLength, maxBytesToConsume);
		} else {
			readsize = readDefaultLine(str, maxLineLength, maxBytesToConsume);
		}

		while (readsize <= 0) {
			if (in instanceof TarInputStream) {// do tar header
				TarEntry te = ((TarInputStream) in).getNextEntry();
				System.out.print("input stream is TarInputStream  getNextEntry:");
				while (te != null && te.isDirectory()) {
					LOG.info(" dir: " + te.getName());
					te = ((TarInputStream) in).getNextEntry();
				}
				if (te == null)
					return -1;
				LOG.info(" file: " + te.getName());
				if (this.perFileSkipRowNum > 0 && !skipFileNum(str, maxLineLength, maxBytesToConsume))
					return -1;
			} else if (in instanceof ZipInputStream) {
				System.out.print("input stream is ZipInputStream  getNextEntry:");
				ZipEntry zin = ((ZipInputStream) in).getNextEntry();
				while (zin != null && zin.isDirectory()) {
					LOG.info(" dir: " + zin.getName());
					zin = ((ZipInputStream) in).getNextEntry();
				}
				if (zin == null)
					return -1;
				if (this.perFileSkipRowNum > 0 && !skipFileNum(str, maxLineLength, maxBytesToConsume))
					return -1;
				LOG.info(" file: " + zin.getName());
			} else {
				break;
			}
			if (this.recordDelimiterBytes != null) {
				readsize = readCustomLine(str, maxLineLength, maxBytesToConsume);
			} else {
				readsize = readDefaultLine(str, maxLineLength, maxBytesToConsume);
			}
		}
		return readsize;
	}

	private boolean skipFileNum(Text str, int maxLineLength, int maxBytesToConsume) throws IOException {
		int rowNum = 0;
		int readsize = 1;
		while (rowNum++ < this.perFileSkipRowNum && readsize > 0) {
			if (this.recordDelimiterBytes != null) {
				readsize = readCustomLine(str, maxLineLength, maxBytesToConsume);
			} else {
				readsize = readDefaultLine(str, maxLineLength, maxBytesToConsume);
			}
		}
		return true;
	}

	/**
	 * Read a line terminated by one of CR, LF, or CRLF.
	 */
	private int readDefaultLine(Text str, int maxLineLength, int maxBytesToConsume) throws IOException {
		/*
		 * We're reading data from in, but the head of the stream may be already
		 * buffered in buffer, so we have several cases: 1. No newline
		 * characters are in the buffer, so we need to copy everything and read
		 * another buffer from the stream. 2. An unambiguously terminated line
		 * is in buffer, so we just copy to str. 3. Ambiguously terminated line
		 * is in buffer, i.e. buffer ends in CR. In this case we copy everything
		 * up to CR to str, but we also need to see what follows CR: if it's LF,
		 * then we need consume LF as well, so next call to readLine will read
		 * from after that. We use a flag prevCharCR to signal if previous
		 * character was CR and, if it happens to be at the end of the buffer,
		 * delay consuming it until we have a chance to look at the char that
		 * follows.
		 */
		str.clear();
		int txtLength = 0; // tracks str.getLength(), as an optimization
		int newlineLength = 0; // length of terminating newline
		boolean prevCharCR = false; // true of prev char was CR
		long bytesConsumed = 0;
		do {
			int startPosn = bufferPosn; // starting from where we left off the
										// last time
			if (bufferPosn >= bufferLength) {
				startPosn = bufferPosn = 0;
				if (prevCharCR)
					++bytesConsumed; // account for CR from previous read
				bufferLength = in.read(buffer);
				if (bufferLength <= 0)
					break; // EOF
			}
			for (; bufferPosn < bufferLength; ++bufferPosn) { // search for
																// newline
				if (buffer[bufferPosn] == LF) {
					newlineLength = (prevCharCR) ? 2 : 1;
					++bufferPosn; // at next invocation proceed from following
									// byte
					break;
				}
				if (prevCharCR) { // CR + notLF, we are at notLF
					newlineLength = 1;
					break;
				}
				prevCharCR = (buffer[bufferPosn] == CR);
			}
			int readLength = bufferPosn - startPosn;
			if (prevCharCR && newlineLength == 0)
				--readLength; // CR at the end of the buffer
			bytesConsumed += readLength;
			int appendLength = readLength - newlineLength;
			if (appendLength > maxLineLength - txtLength) {
				appendLength = maxLineLength - txtLength;
			}
			if (appendLength > 0) {
				str.append(buffer, startPosn, appendLength);
				txtLength += appendLength;
			}
		} while (newlineLength == 0 && bytesConsumed < maxBytesToConsume);

		if (bytesConsumed > (long) Integer.MAX_VALUE)
			throw new IOException("Too many bytes before newline: " + bytesConsumed);
		return (int) bytesConsumed;
	}

	/**
	 * Read a line terminated by a custom delimiter.
	 */
	private int readCustomLine(Text str, int maxLineLength, int maxBytesToConsume) throws IOException {
		str.clear();
		int txtLength = 0; // tracks str.getLength(), as an optimization
		long bytesConsumed = 0;
		int delPosn = 0;
		do {
			int startPosn = bufferPosn; // starting from where we left off the
										// last
			// time
			if (bufferPosn >= bufferLength) {
				startPosn = bufferPosn = 0;
				bufferLength = in.read(buffer);
				if (bufferLength <= 0)
					break; // EOF
			}
			for (; bufferPosn < bufferLength; ++bufferPosn) {
				if (buffer[bufferPosn] == recordDelimiterBytes[delPosn]) {
					delPosn++;
					if (delPosn >= recordDelimiterBytes.length) {
						bufferPosn++;
						break;
					}
				} else {
					delPosn = 0;
				}
			}
			int readLength = bufferPosn - startPosn;
			bytesConsumed += readLength;
			int appendLength = readLength - delPosn;
			if (appendLength > maxLineLength - txtLength) {
				appendLength = maxLineLength - txtLength;
			}
			if (appendLength > 0) {
				str.append(buffer, startPosn, appendLength);
				txtLength += appendLength;
			}
		} while (delPosn < recordDelimiterBytes.length && bytesConsumed < maxBytesToConsume);
		if (bytesConsumed > (long) Integer.MAX_VALUE)
			throw new IOException("Too many bytes before delimiter: " + bytesConsumed);
		return (int) bytesConsumed;
	}

	/**
	 * Read from the InputStream into the given Text.
	 * 
	 * @param str
	 *            the object to store the given line
	 * @param maxLineLength
	 *            the maximum number of bytes to store into str.
	 * @return the number of bytes read including the newline
	 * @throws IOException
	 *             if the underlying stream throws
	 */
	public int readLine(Text str, int maxLineLength) throws IOException {
		return readLine(str, maxLineLength, Integer.MAX_VALUE);
	}

	/**
	 * Read from the InputStream into the given Text.
	 * 
	 * @param str
	 *            the object to store the given line
	 * @return the number of bytes read including the newline
	 * @throws IOException
	 *             if the underlying stream throws
	 */
	public int readLine(Text str) throws IOException {
		return readLine(str, Integer.MAX_VALUE, Integer.MAX_VALUE);
	}

}
