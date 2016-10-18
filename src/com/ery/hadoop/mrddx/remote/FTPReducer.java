package com.ery.hadoop.mrddx.remote;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.ery.hadoop.mrddx.DBRecord;

public class FTPReducer extends Reducer<DBRecord, DBRecord, DBRecord, NullWritable> {

	// @Override
	// protected void reduce(DBRecord key, Iterable<DBRecord> values, Context
	// context) throws IOException,
	// InterruptedException {
	// for (DBRecord value : values) {
	// context.write((DBRecord) key, NullWritable.get());
	// }
	// }

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		// NOTHING
	}
}
