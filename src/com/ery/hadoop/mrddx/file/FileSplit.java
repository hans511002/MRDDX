package com.ery.hadoop.mrddx.file;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;

import com.ery.hadoop.mrddx.util.StringUtil;

@SuppressWarnings("deprecation")
public class FileSplit extends InputSplit implements org.apache.hadoop.mapred.InputSplit {
	private Path[] file;
	private long[] start;
	// private String tmpStringStart;
	private long[] length;
	// private String tmpStringLength;
	private String[] hosts;
	// private String tmpStringHost;
	private long[] fileLogId;
	// private String tmpStringFileLogId;
	private boolean isDeleteBeforeData;
	private String[] fileId;
	// private String tmpStringFileId;
	private int randomNumber = 0;
	private int fileIndex = 0;

	public FileSplit() {
	}

	public FileSplit(List<InputSplit> lstSplit, String hosts) {
		int len = lstSplit.size();
		this.file = new Path[len];
		this.start = new long[len];
		this.length = new long[len];
		if (hosts != null)
			this.hosts = new String[] { hosts };
		else
			this.hosts = null;
		long tmpFileLogId[] = new long[len];
		String tmpFileId[] = new String[len];
		for (int j = 0; j < lstSplit.size(); j++) {
			FileSplits fileSplits = (FileSplits) lstSplit.get(j);
			this.file[j] = fileSplits.getPath();
			this.start[j] = fileSplits.getStart();
			this.length[j] = fileSplits.getLength();
			tmpFileLogId[j] = fileSplits.getFileLogId();
			tmpFileId[j] = fileSplits.getFileId();
		}

		// this.tmpStringStart = StringUtil.parseArrayToString(start, ",");
		// this.tmpStringLength = StringUtil.parseArrayToString(length, ",");
		// try {
		// this.tmpStringHost = StringUtil.serialObject(this.hosts, false);
		// } catch (IOException e) {
		// this.tmpStringHost = null;
		// e.printStackTrace();
		// }

		this.setFileId(tmpFileId);
		this.setFileLogId(tmpFileLogId);
	}

	public long getFileLogId() {
		return fileLogId[fileIndex];
	}

	public void setFileLogId(long[] fileLogId) {
		this.fileLogId = fileLogId;
		// tmpStringFileLogId = StringUtil.parseArrayToString(fileLogId, ",");
	}

	public boolean isDeleteBeforeData() {
		return isDeleteBeforeData;
	}

	public void setDeleteBeforeData(boolean isDeleteBeforeData) {
		this.isDeleteBeforeData = isDeleteBeforeData;
	}

	public void setFileId(String[] fileId) {
		this.fileId = fileId;
		// try {
		// this.tmpStringFileId = StringUtil.serialObject(fileId, false);
		// } catch (IOException e) {
		// this.tmpStringFileId = null;
		// e.printStackTrace();
		// }
	}

	public String getFileId() {
		return fileId[fileIndex];
	}

	public int getRandomNumber() {
		return randomNumber;
	}

	public void setRandomNumber(int randomNumber) {
		this.randomNumber = randomNumber;
	}

	/**
	 * Constructs a split.
	 * 
	 * @deprecated
	 * @param file
	 *            the file name
	 * @param start
	 *            the position of the first byte in the file to process
	 * @param length
	 *            the number of bytes in the file to process
	 */
	// @Deprecated
	// public FileSplit(Path[] file, long[] start, long[] length, JobConf conf)
	// {
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
	public FileSplit(Path[] file, long[] start, long[] length, String[] fileIds, long[] fileLogIds, String[] hosts) {
		this.file = file;
		this.start = start;
		this.length = length;
		this.hosts = hosts;
		this.fileId = fileIds;
		this.fileLogId = fileLogIds;
		// this.tmpStringStart = StringUtil.parseArrayToString(start, ",");
		// this.tmpStringLength = StringUtil.parseArrayToString(length, ",");
		// try {
		// this.tmpStringHost = StringUtil.serialObject(hosts, false);
		// } catch (IOException e) {
		// this.tmpStringHost = null;
		// e.printStackTrace();
		// }
	}

	/** The file containing this split's data. */
	public Path getPath() {
		return file[fileIndex];
	}

	/** The position of the first byte in the file to process. */
	public long getStart() {
		return start[fileIndex];
	}

	/** The number of bytes in the file to process. */
	public long getLength() {
		return length[fileIndex];
	}

	/** The number of bytes in the file to process. */
	public long[] getLengths() {
		return length;
	}

	public void setFileIndex(int fileIndex) {
		if (fileIndex >= 0 && fileIndex < this.getFileMaxIndex()) {
			this.fileIndex = fileIndex;
		}
	}

	public int getFileIndex() {
		return fileIndex;
	}

	public int getFileMaxIndex() {
		return file.length;
	}

	public String toString() {
		StringBuilder res = new StringBuilder();
		if (hosts != null) {
			res.append("\nhosts=" + StringUtil.parseArrayToString(hosts, ",") + "\n");
		}
		for (int i = 0; i < file.length; i++) {
			res.append("index " + i + "   :");
			res.append(file[i]);
			res.append(",");
			res.append(start[i]);
			res.append(",");
			res.append(length[i]);
			res.append(",");
			res.append(this.fileLogId[i]);
			res.append(",");
			res.append(this.fileId[i]);
			res.append("\n");
		}
		return res.toString();
	}

	public String getPathString() {
		StringBuilder str = new StringBuilder();
		for (int i = 0; i < file.length; i++) {
			if (file[i] == null) {
				continue;
			}
			str.append(file[i].toString());
			if (i != file.length - 1) {
				str.append(" ");
			}
		}
		return str.toString();
	}

	public Path[] getParaPath(String pathStr) {
		String[] paths = pathStr.split(" ");
		Path[] path = new Path[paths.length];
		for (int i = 0; i < paths.length; i++) {
			path[i] = new Path(paths[i]);
		}
		return path;
	}

	// //////////////////////////////////////////
	// Writable methods
	// //////////////////////////////////////////

	public void write(DataOutput out) throws IOException {
		out.writeInt(this.file.length);
		out.writeBoolean(this.isDeleteBeforeData);
		for (int i = 0; i < this.file.length; i++) {
			Text.writeString(out, this.file[i].toString());
			out.writeLong(start[i]);
			out.writeLong(length[i]);
			out.writeLong(fileLogId[i]);
			Text.writeString(out, this.fileId[i]);
			if (this.hosts == null || this.hosts[i] == null) {
				Text.writeString(out, "");
			} else {
				Text.writeString(out, this.hosts[i]);
			}
		}
	}

	public void readFields(DataInput in) throws IOException {
		int len = in.readInt();
		file = new Path[len];
		start = new long[len];
		length = new long[len];
		fileLogId = new long[len];
		fileId = new String[len];
		this.hosts = new String[len];
		this.isDeleteBeforeData = in.readBoolean();
		boolean hostsIsNull = true;
		for (int i = 0; i < this.file.length; i++) {
			this.file[i] = new Path(Text.readString(in));
			start[i] = in.readLong();
			length[i] = in.readLong();
			fileLogId[i] = in.readLong();
			this.fileId[i] = Text.readString(in);
			String host = Text.readString(in);
			if (host != null && !host.trim().equals("")) {
				this.hosts[i] = host;
				hostsIsNull = false;
			}
		}
		if (hostsIsNull)
			this.hosts = null;
	}

	public String[] getLocations() {
		if (this.hosts == null) {
			return new String[] {};
		} else {
			return this.hosts;
		}
	}
}
