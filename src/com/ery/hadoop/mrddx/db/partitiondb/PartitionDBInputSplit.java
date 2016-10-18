/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- Java -*- */

package com.ery.hadoop.mrddx.db.partitiondb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.ery.hadoop.mrddx.db.DBInputFormat;
import com.ery.hadoop.mrddx.vertica.Relation;

/**
 * Input split class for reading data from Vertica
 * 
 */
public class PartitionDBInputSplit extends DBInputFormat.DBInputSplit implements Writable {
	private static final Log LOG = LogFactory.getLog(PartitionDBInputSplit.class);
	String hostUrl = null;
	String inputQuery = null;
	List<Object> segmentParams = null;

	public PartitionDBInputSplit() {
		LOG.trace("Input split default constructor");
	}

	public PartitionDBInputSplit(String hostUrl, String inputQuery, List<Object> segmentParams) {
		if (LOG.isDebugEnabled()) {
			StringBuilder sb = new StringBuilder();
			sb.append("Input split on[" + hostUrl + "] with query -");
			sb.append(inputQuery);
			sb.append("-, Parameters: ");
			if (segmentParams != null) {
				boolean addComma = false;
				for (Object param : segmentParams) {
					if (addComma)
						sb.append(",");
					sb.append(param.toString());
					addComma = true;
				}
			}
			LOG.debug(sb.toString());
		}
		this.hostUrl = hostUrl;
		this.inputQuery = inputQuery;
		this.segmentParams = segmentParams;
	}

	public PartitionDBInputSplit(String hostUrl, String inputQuery, long start, long end) {
		LOG.debug("Input split on[" + hostUrl + "] with query -" + inputQuery + "-, start row: " + start +
				" and end row: " + end);
		this.hostUrl = hostUrl;
		this.inputQuery = inputQuery;
		this.start = start;
		this.end = end;
	}

	public String getHostUrl() {
		return this.hostUrl;
	}

	public List<Object> getSegmentParams() {
		return segmentParams;
	}

	public long getLength() {
		return end - start;
	}

	public String[] getLocations() throws IOException {
		return new String[] {};
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		hostUrl = Text.readString(in);
		inputQuery = Text.readString(in);
		segmentParams = null;
		long paramCount = in.readLong();
		LOG.debug("Reading " + paramCount + " parameters");
		if (paramCount > 0) {
			int type = in.readInt();
			segmentParams = new ArrayList<Object>();
			for (int i = 0; i < paramCount; i++) {
				segmentParams.add(Relation.readField(type, in));
			}
		}
		start = in.readLong();
		end = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, hostUrl);
		Text.writeString(out, inputQuery);
		if (segmentParams != null && segmentParams.size() > 0) {
			LOG.debug("Writing out " + segmentParams.size() + " parameters");
			out.writeLong(segmentParams.size());
			int type = Relation.getType(segmentParams.get(0));
			out.writeInt(type);
			for (Object o : segmentParams)
				Relation.write(o, type, out);
		} else {
			LOG.debug("Writing out no parameters");
			out.writeLong(0);
		}
		out.writeLong(start);
		out.writeLong(end);
	}

}
