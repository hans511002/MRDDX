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

package com.ery.hadoop.mrddx.hive;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.ery.hadoop.mrddx.db.mapreduce.FileWritable;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.HDFSUtils;

/**
 * Hive针对RCfile的输出格式
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-1-18
 * @version v1.0
 * @param <K>
 * @param <V>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class HiveRCFileOutputFormat<K extends FileWritable, V> extends HiveOutputFormat<K, V> {
	private static final Log LOG = LogFactory.getLog(HiveOutputFormat.class);

	public static void setColumnNumber(Configuration conf, int columnNum) {
		assert columnNum > 0;
		conf.setInt(RCFile.COLUMN_NUMBER_CONF_STR, columnNum);
	}

	@Override
	public RecordWriter<K, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new HiveRCFileRecordWriter<K, NullWritable>(context, this);

		// // public RecordWriter<K, NullWritable> getRecordWriter(FileSystem
		// // ignored, JobConf job, String name,
		// // Progressable progress) throws IOException {
		// HiveConfiguration hiveConf = new HiveConfiguration(job);
		//
		// String fieldSeparator = hiveConf.getOutputHiveFileFieldSplitChars();
		// String rowSeparator = hiveConf.getOutputHiveFileRowsSplitChars();
		// String[] fieldNames = hiveConf.getOutputFieldNames();
		// boolean isCompressed = hiveConf.getOutputHiveCompress();
		//
		// // 无需压缩
		// if (!isCompressed) {
		// Path file = FileOutputFormat.getTaskOutputPath(job, name);
		// FileSystem fs = file.getFileSystem(job);
		// RCFile.Writer out = new RCFile.Writer(fs, job, file, progress, null);
		// return new HiveRCFileRecordWriter<K, NullWritable>(context );
		// }
		//
		// // 需要压缩
		// // 获取压缩标识
		// String compresseCodec = hiveConf.getOutputHiveCompressCodec();
		// CompressionCodec codec = HDFSUtils.getCompressCodec(compresseCodec,
		// job);
		//
		// // build the filename including the extension
		// Path file = FileOutputFormat.getTaskOutputPath(job, name +
		// codec.getDefaultExtension());
		// FileSystem fs = file.getFileSystem(job);
		// RCFile.Writer out = new RCFile.Writer(fs, job, file, progress,
		// codec);
		// return new HiveRCFileRecordWriter<K, NullWritable>(context);
	}

	@Override
	public void handle(Job conf) throws Exception {
		super.handle(conf);
		HiveConfiguration hconf = new HiveConfiguration(conf.getConfiguration());
		// 目标文件压缩方式 (压缩格式：HDFSUtils.CompressCodec)
		String outCompressCodec = hconf.getOutputHiveCompressCodec();
		// 判断是否为BZip2Codec
		if (HDFSUtils.isBZip2CompressCodec(outCompressCodec)) {
			String meg = "[MR ERROR]目标文件压缩方式<" + HiveConfiguration.OUTPUT_HIVE_COMPRESS_CODEC + ">不支持BZip2Codec.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		setColumnNumber(conf.getConfiguration(), hconf.getOutputFieldNames().length);
		conf.setOutputFormatClass(HiveRCFileOutputFormat.class);
	}
}
