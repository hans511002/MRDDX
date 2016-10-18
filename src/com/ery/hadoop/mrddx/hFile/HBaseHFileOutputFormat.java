package com.ery.hadoop.mrddx.hFile;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.IHandleFormat;
import com.ery.hadoop.mrddx.hbase.HbaseConfiguration;
import com.ery.hadoop.mrddx.hbase.HbaseOutputFormat;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.StringUtil;

public class HBaseHFileOutputFormat extends HFileOutputFormat2 implements IHandleFormat {
	private static final Log LOG = LogFactory.getLog(HbaseOutputFormat.class);

	@Override
	public void handle(Job conf) throws Exception {
		// hbase table;
		HbaseConfiguration hConf = new HbaseConfiguration(conf.getConfiguration(), HbaseConfiguration.FLAG_HBASE_OUTPUT);

		// 输出表名
		String tableName = hConf.getOutputHBaseTableName();
		if (null == tableName || tableName.trim().length() <= 0) {
			String meg = "HBase输出表名<" + HbaseConfiguration.OUTPUT_TABLE + ">不能为空.";
			LOG.error(meg);
			throw new Exception(meg);
		}

		Configuration hbconf = HBaseConfiguration.create();
		hbconf.set("hbase.zookeeper.quorum", hConf.getConf().get("hbase.zookeeper.quorum"));
		hbconf.set("zookeeper.znode.parent", hConf.getConf().get("zookeeper.znode.parent"));
		hbconf.setInt("hbase.zookeeper.property.clientPort",
				hConf.getConf().getInt("hbase.zookeeper.property.clientPort", 2181));
		hbconf.setLong("hbase.client.scanner.caching", hConf.getConf().getLong("hbase.client.scanner.caching", 1000));
		HBaseAdmin hbaseAdmin = new HBaseAdmin(hbconf);
		if (!hbaseAdmin.tableExists(tableName)) {
			String meg = "HBase输出表名<" + HbaseConfiguration.OUTPUT_TABLE + ">不存在.";
			LOG.error(meg);
			throw new Exception(meg);
		}

		if (!hbaseAdmin.isTableEnabled(tableName)) {
			String meg = "HBase输出表名<" + HbaseConfiguration.OUTPUT_TABLE + "> not enabled.";
			LOG.error(meg);
			throw new Exception(meg);
		}

		Collection<HColumnDescriptor> family = hbaseAdmin.getTableDescriptor(tableName.getBytes()).getFamilies();
		List<String> lstFamilyName = new ArrayList<String>();
		for (HColumnDescriptor f : family) {
			lstFamilyName.add(f.getNameAsString());
		}

		// 检查拆分字段是否包含在输出字段中
		String outColumnRelation = hConf.getOutputHBaseColumnRelation();
		if (null == outColumnRelation) {
			String meg = "[MR ERROR]拆分字段<" + HbaseConfiguration.OUTPUT_HBASE_COLUMN_RELATION + ">不能为空";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		List<String[]> list = new ArrayList<String[]>();// 获取列簇与列的对应关系
		List<String[]> rela = new ArrayList<String[]>();
		StringUtil.decodeOutColumnSplitRelation(outColumnRelation, list, rela);
		String[][] clusterFieldNames = list.toArray(new String[0][0]);
		String[][] outColumnSplitRelations = rela.toArray(new String[0][0]);
		if (clusterFieldNames.length <= 0 || outColumnSplitRelations.length <= 0) {
			String meg = "[MR ERROR]拆分字段<" + HbaseConfiguration.OUTPUT_HBASE_COLUMN_RELATION + ">不正确";
			MRLog.error(LOG, meg);
			throw new Exception(meg);
		}

		// 验证拆分列簇与列字段
		for (int i = 0; i < clusterFieldNames.length; i++) {
			String tmp = clusterFieldNames[i][0];
			if (!lstFamilyName.contains(tmp)) {
				String meg = "[MR ERROR]拆分字段<" + HbaseConfiguration.OUTPUT_HBASE_COLUMN_RELATION + ">的列簇<" + tmp
						+ ">不存在";
				LOG.error(meg);
				throw new Exception(meg);
			}
		}

		// 验证拆分列字段
		String splitSign = hConf.getOutputHBaseColumnSplitSign();
		Set<String> setTargetFiled = StringUtil.parseStringArrayToSet(hConf.getOutputFieldNames());
		for (int i = 0; i < outColumnSplitRelations.length; i++) {
			String temp[] = outColumnSplitRelations[i];
			for (int j = 0; j < temp.length; j++) {
				if (!setTargetFiled.contains(temp[j])) {
					String meg = "[MR ERROR]<" + HbaseConfiguration.OUTPUT_HBASE_COLUMN_RELATION + ">字段中列<" + temp[j]
							+ ">不在输出字段<" + HbaseConfiguration.SYS_OUTPUT_FIELD_NAMES_PROPERTY + ">中";
					LOG.error(meg);
					throw new Exception(meg);
				}
			}

			if (temp.length <= 0) {
				String meg = "[MR ERROR]<" + HbaseConfiguration.OUTPUT_HBASE_COLUMN_RELATION + ">字段中存在不包含输出字段的列簇与列的关系";
				LOG.error(meg);
				throw new Exception(meg);
			}

			if (temp.length > 1 && (null == splitSign || splitSign.trim().length() <= 0)) {
				String meg = "[MR ERROR]<" + HbaseConfiguration.OUTPUT_HBASE_COLUMN_RELATION
						+ ">字段中存在输出字段数量大于1, 需要设置分隔符<" + HbaseConfiguration.OUTPUT_HBASE_COLUMN_SPLIT_SIGN + ">";
				LOG.error(meg);
				throw new Exception(meg);
			}
		}

		// 行规则
		String rowKeyRule = hConf.getOutputHBaseRowKeyRule();
		if (null == rowKeyRule || rowKeyRule.trim().length() <= 0) {
			String meg = "表的行规则<" + HbaseConfiguration.OUTPUT_ROWKEY_RULE + ">未设置";
			LOG.error(meg);
			throw new Exception(meg);
		}

		conf.setReducerClass(KeyValueSortReducer.class);
		conf.setOutputFormatClass(HBaseHFileOutputFormat.class);
		conf.setReduceSpeculativeExecution(false);
		conf.setOutputKeyClass(DBRecord.class);
		conf.setOutputValueClass(NullWritable.class);
		HTable table = new HTable(hbconf, tableName);
		HBaseHFileOutputFormat.configureIncrementalLoad(conf, table);
	}
}
