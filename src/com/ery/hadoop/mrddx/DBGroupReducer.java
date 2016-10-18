package com.ery.hadoop.mrddx;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

import com.ery.hadoop.mrddx.hive.HiveConfiguration;
import com.ery.hadoop.mrddx.util.StringUtil;

/**
 * Map产生的中间结果合并处理类 Copyrights @ 2012,Tianyuan DIC Information Co.,Ltd. All
 * rights reserved.
 * 
 * @author wanghao
 * @version v1.0
 * @create Data 2013-1-9
 */
public class DBGroupReducer extends Reducer<DBRecord, DBRecord, DBRecord, DBRecord> {
	// 是否map直接输出结果（true：直接输出）
	private boolean inputMapEnd;

	// 分组方法队列
	public static HashMap<String, Integer> GROUP_METHODS = new HashMap<String, Integer>();

	// 分组行记录数标识符
	public static String GROUPBY_COUNTKEY = "__GROUP_ROW_COUNT";

	// 分组字段队列
	private String[] groupFieldMethod = null;

	// 目标字段队列
	private String[] destFieldNames = null;

	/**
	 * 针对直接生成文件功能
	 */
	// 分区字段名称
	private String[] partitionField;

	static {
		GROUP_METHODS.put("NONE", 0);
		GROUP_METHODS.put("SUM", 1);
		GROUP_METHODS.put("AVG", 2);
		GROUP_METHODS.put("MIN", 3);
		GROUP_METHODS.put("MAX", 4);
		GROUP_METHODS.put("COUNT", 5);
	}

	@Override
	protected void reduce(DBRecord key, Iterable<DBRecord> values, Context context) throws IOException,
			InterruptedException {
		for (DBRecord value : values) {
			context.write((DBRecord) key, (DBRecord) value);
		}
		DBRecord sum = new DBRecord();
		Object sumObjField = null;
		int rowCount = 0;
		for (DBRecord nrow : values) {
			if (nrow.isNull()) {
				continue;
			}
			rowCount++;
			// 处理统计函数
			for (int i = 0; i < this.destFieldNames.length; i++) {
				Integer method = DBGroupReducer.GROUP_METHODS.get(this.groupFieldMethod[i]);
				String fieldName = this.destFieldNames[i];
				if (method == 0) {
					continue;
				}

				sumObjField = sum.getData(fieldName);
				Object v = nrow.getData(fieldName);
				switch (method) {
				case 1: // SUM
				case 2: // AVG
					Double d = null == v ? 0 : StringUtil.stringToDouble(v.toString());
					Double objv = null == sumObjField ? 0 : StringUtil.stringToDouble(sumObjField.toString());
					sum.putData(fieldName, d + objv);
					break;
				case 3: // MIN
					sum.putData(fieldName, StringUtil.getSmallObject(v, sumObjField));
					break;
				case 4: // MAX
					sum.putData(fieldName, StringUtil.getBigObject(v, sumObjField));
					break;
				case 5: // COUNT
					Object obj = sum.getData(fieldName);
					sum.putData(fieldName, null == obj ? 1 : (StringUtil.objectToInt(obj)) + 1);
					break;
				}
			}
		}
		// 处理AVG
		for (int i = 0; i < this.destFieldNames.length; i++) {
			Integer method = DBGroupReducer.GROUP_METHODS.get(groupFieldMethod[i]);
			String fieldName = this.destFieldNames[i];
			if (method != 2) {
				continue;
			}
			sumObjField = sum.getData(fieldName);
			if (null != sumObjField) {
				double rsum = StringUtil.stringToDouble(sumObjField);
				if (rowCount > 0) {
					sum.putData(fieldName,
							StringUtil.doubleToString(rsum / rowCount, StringUtil.DOUBLE_FORMAT_8Decimal));
				}
			}
		}
		if (this.partitionField == null || this.partitionField.length <= 0) {
			sum.putData(GROUPBY_COUNTKEY, rowCount);
			// output.collect(key, sum);
			context.write(key, sum);
		} else {
			DBRecord[] gps = key.splitPartition(this.destFieldNames, sum, this.partitionField);
			// output.collect(gps[0], gps[1]); // 分区输出
			context.write(gps[0], gps[1]);
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// // NOTHING
		// }
		// public void configure(JobConf job) {
		Configuration job = context.getConfiguration();
		MRConfiguration conf = new MRConfiguration(job);
		this.inputMapEnd = conf.getInputMapEnd();// 是否map直接输出结果
		String[] srcFieldNames = conf.getInputFieldNames();// 输入
		String[] targetFieldNames = conf.getOutputFieldNames();// 输出
		if (targetFieldNames == null) {
			targetFieldNames = srcFieldNames.clone();
		}

		HiveConfiguration hconf = new HiveConfiguration(job);
		this.partitionField = hconf.getOutputHivePartitionField(); // hive分区字段

		this.groupFieldMethod = conf.getRelatedGroupFieldMethod();// 目标到源映射及方法
		Set<String> setSrcFieldNames = new HashSet<String>();
		CollectionUtils.addAll(setSrcFieldNames, srcFieldNames);

		Set<String> setTargetFieldNames = new HashSet<String>();
		CollectionUtils.addAll(setTargetFieldNames, targetFieldNames);

		if (!this.inputMapEnd) {
			analyzeConfig(job, setSrcFieldNames, setTargetFieldNames);
		}
	}

	/**
	 * 分析配置信息
	 * 
	 * @param job
	 *            job对象
	 * @param setSrcFieldNames
	 *            输入字段
	 * @param setTargetFieldNames
	 *            输出字段
	 */
	private void analyzeConfig(Configuration job, Set<String> setSrcFieldNames, Set<String> setTargetFieldNames) {
		this.destFieldNames = new String[groupFieldMethod.length];
		for (int i = 0; i < groupFieldMethod.length; i++) {
			String[] tmp = groupFieldMethod[i].split(":");
			if (tmp.length != 3) {
				throw new RuntimeException("分组计算中目标到源映射及方法配置不正确:" +
						job.get(MRConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL));
			}

			if (!setTargetFieldNames.contains(tmp[0])) {
				throw new RuntimeException("配置错误,目标字段" + tmp[0] + "不存在于输出字段 :" +
						job.get(MRConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL));
			}

			if (!setSrcFieldNames.contains(tmp[1])) {
				throw new RuntimeException("配置错误,源字段" + tmp[1] + "不存在输入字段 :" +
						job.get(MRConfiguration.SYS_RELATED_GROUP_BY_FIELD_METHOD_REL));
			}

			this.destFieldNames[i] = tmp[0];
			this.groupFieldMethod[i] = tmp[2];
		}
	}
}
