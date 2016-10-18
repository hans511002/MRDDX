package com.ery.hadoop.mrddx;

import org.apache.hadoop.conf.Configuration;

public interface IMROutputFormatEnd {
	public void processEnd(Configuration conf) throws Exception;
}
