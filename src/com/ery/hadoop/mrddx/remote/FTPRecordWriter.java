package com.ery.hadoop.mrddx.remote;

import java.io.IOException;

import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

public class FTPRecordWriter<K extends FTPWritable, V> implements RecordWriter<K, V> {

	@Override
	public void write(K key, V value) throws IOException {
	}

	@Override
	public void close(Reporter reporter) throws IOException {
	}
}
