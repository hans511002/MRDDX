package com.ery.hadoop.mrddx.file;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;

public class FileSplits extends org.apache.hadoop.mapreduce.lib.input.FileSplit implements InputSplit {
	private long fileLogId;
	private boolean isDeleteBeforeData;
	private String fileId;
	private int randomNumber;

	public FileSplits() {
		super();
	}

	public long getFileLogId() {
		return fileLogId;
	}

	public void setFileLogId(long fileLogId) {
		this.fileLogId = fileLogId;
	}

	public boolean isDeleteBeforeData() {
		return isDeleteBeforeData;
	}

	public void setDeleteBeforeData(boolean isDeleteBeforeData) {
		this.isDeleteBeforeData = isDeleteBeforeData;
	}

	public void setFileId(String fileId) {
		this.fileId = fileId;
	}

	public String getFileId() {
		return fileId;
	}

	public int getRandomNumber() {
		return randomNumber;
	}

	public void setRandomNumber(int randomNumber) {
		this.randomNumber = randomNumber;
	}

	// /**
	// * Constructs a split.
	// *
	// * @deprecated
	// * @param file the file name
	// * @param start the position of the first byte in the file to process
	// * @param length the number of bytes in the file to process
	// */
	// @Deprecated
	// public FileSplits(Path file, long start, long length, JobConf conf) {
	// this(file, start, length, (String[]) null);
	// }

	/**
	 * Constructs a split with host information
	 * 
	 * @param file
	 *            the file name
	 * @param start
	 *            the position of the first byte in the file to process
	 * @param length
	 *            the number of bytes in the file to process
	 * @param hosts
	 *            the list of hosts containing the block, possibly null
	 */
	public FileSplits(Path file, long start, long length, String[] hosts, String[] inMemoryHosts) {
		super(file, start, length, hosts, inMemoryHosts);
	}

	public FileSplits(Path file, long start, long length, String[] hosts) {
		super(file, start, length, hosts);
	}

	public String toString() {
		return this.getPath() + ":" + this.getStart() + "+" + this.getLength() + " " + this.fileLogId + " " +
				this.fileId;
	}

	public String getPathString() {
		return getPath().toString();
	}

	// //////////////////////////////////////////
	// Writable methods
	// //////////////////////////////////////////

	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeLong(this.fileLogId);
		out.writeBoolean(this.isDeleteBeforeData);
	}

	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.fileLogId = in.readLong();
		this.isDeleteBeforeData = in.readBoolean();
	}

	public FileSplits clone(String[] newhost, String[] inMemoryHosts) {
		FileSplits is = null;
		if (inMemoryHosts == null) {
			is = new FileSplits(this.getPath(), this.getStart(), this.getLength(), newhost);
		} else {
			is = new FileSplits(this.getPath(), this.getStart(), this.getLength(), inMemoryHosts);
		}
		is.fileId = this.fileId;
		is.fileLogId = this.fileLogId;
		return is;
	}

}
