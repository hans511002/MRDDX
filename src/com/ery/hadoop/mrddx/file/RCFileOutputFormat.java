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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.ery.hadoop.mrddx.DBReducer;
import com.ery.hadoop.mrddx.IHandleFormat;
import com.ery.hadoop.mrddx.hive.HiveConfiguration;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.HDFSUtils;

/**
 * RCFile输出格式
 * 



 * @createDate 2013-1-10
 * @version v1.0
 * @param <K>
 * @param <V>
 */
public class RCFileOutputFormat<K, V> extends FileOutputFormat<K, V> implements IHandleFormat {
	public static final Log LOG = LogFactory.getLog(RCFileOutputFormat.class);

	public static void setColumnNumber(Configuration conf, int columnNum) {
		assert columnNum > 0;
		conf.setInt(RCFile.COLUMN_NUMBER_CONF_STR, columnNum);
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		return new RCFileRecordWriter<K, V>(context, this);
	}

	/**
	 * 设置输出参数
	 * 
	 * @param conf 配置对象
	 * @param pOutputCompress 是否被压缩
	 * @param pOutputCompressCodec 压缩方式
	 * @param pOutputFileFieldSplitChars 列分隔符
	 * @param pOutputFileRowsSplitChars 行分隔符
	 * @throws Exception 异常
	 */
	public static void setOutputParameter(Configuration conf, boolean pOutputCompress, String pOutputCompressCodec,
			String pOutputFileFieldSplitChars, String pOutputFileRowsSplitChars) throws Exception {
		FileConfiguration dbconf = new FileConfiguration(conf, FileConfiguration.FLAG_FILE_OUTPUT);
		dbconf.setOutputFileCompress(pOutputCompress);
		dbconf.setOutputFileCompressCodec(pOutputCompressCodec);
		dbconf.setOutputFileFieldSplitChars(pOutputFileFieldSplitChars);
		dbconf.setOutputFileRowsSplitChars(pOutputFileRowsSplitChars);
	}

	@Override
	public void handle(Job conf) throws Exception {
		/**
		 * 校验参数
		 */
		FileConfiguration dbconf = new FileConfiguration(conf.getConfiguration(), FileConfiguration.FLAG_FILE_OUTPUT);
		// 获取目标文件行分隔符
		String outRowChars = dbconf.getOutputFileRowsSplitChars();
		if (null == outRowChars || outRowChars.length() <= 0) {
			String meg = "输出行分隔符<" + FileConfiguration.OUTPUT_FILE_ROWS_SPLIT_CHARS + ">未设置";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		// 目标文件列分隔符
		String outFileSplitChars = dbconf.getOutputFileFieldSplitChars();
		if (null == outFileSplitChars || outFileSplitChars.trim().length() <= 0) {
			String meg = "输出行列隔符<" + FileConfiguration.OUTPUT_FILE_FIELD_SPLIT_CHARS + ">未设置";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		boolean para = dbconf.getOutputFileCompress();
		// 目标文件压缩方式 (压缩格式：HDFSUtils.CompressCodec)
		String outCompressCodec = dbconf.getOutputFileCompressCodec();
		if (para && !HDFSUtils.isExistCompressCodec(outCompressCodec)) {
			String meg = "[MR ERROR]目标文件压缩方式<" + FileConfiguration.OUTPUT_FILE_COMPRESSCODEC + ">设置不正确.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		// 判断是否为BZip2Codec
		if (HDFSUtils.isBZip2CompressCodec(outCompressCodec)) {
			String meg = "[MR ERROR]目标文件压缩方式<" + HiveConfiguration.OUTPUT_HIVE_COMPRESS_CODEC + ">不支持BZip2Codec.";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		// 获取MR最终存放结果的目录
		String outTargetpath = dbconf.getOutputTargetFilePath();
		dbconf.setOutputTargetPath(outTargetpath);
		if (null == outTargetpath || outTargetpath.trim().length() <= 0) {
			MRLog.warn(LOG, "MR最终存放结果的目录<" + FileConfiguration.OUTPUT_FILE_TARGET_PATH + ">为空");
		}

		setColumnNumber(conf.getConfiguration(), dbconf.getOutputFieldNames().length);
		conf.setOutputFormatClass(RCFileOutputFormat.class);
		conf.setReducerClass(DBReducer.class);
	}
}
