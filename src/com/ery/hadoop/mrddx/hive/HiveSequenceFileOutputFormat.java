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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.ery.hadoop.mrddx.db.mapreduce.FileWritable;

/**
 * Hive针对Sequenc文件的输出格式
 * 



 * @createDate 2013-1-29
 * @version v1.0
 * @param <K>
 * @param <V>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class HiveSequenceFileOutputFormat<K extends FileWritable, V> extends HiveOutputFormat<K, V> {
	@Override
	public RecordWriter<K, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException,
			InterruptedException {

		return new HiveSequenceFileRecordWriter<K, NullWritable>(context, this);

	}

	@Override
	public void handle(Job conf) throws Exception {
		super.handle(conf);
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setOutputFormatClass(HiveSequenceFileOutputFormat.class);
	}
}
