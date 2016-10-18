package com.ery.hadoop.mrddx.hFile;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;

import com.ery.hadoop.mrddx.FileMapper;
import com.ery.hadoop.mrddx.IHandleFormat;

public class HBaseHFileInputFormat extends TextInputFormat implements IHandleFormat {

	@Override
	public void handle(Job conf) throws Exception {
		super.handle(conf);
		conf.setMapperClass(FileMapper.class);
		conf.setMapperClass(DBHFileMapper.class);
		conf.setInputFormatClass(HBaseHFileInputFormat.class);

		conf.setMapOutputKeyClass(ImmutableBytesWritable.class);
		conf.setMapOutputValueClass(KeyValue.class);
	}
}
