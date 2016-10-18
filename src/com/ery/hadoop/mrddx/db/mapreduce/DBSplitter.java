package com.ery.hadoop.mrddx.db.mapreduce;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * DBSplitter will generate DBInputSplits to use with DataDrivenDBInputFormat.
 * DataDrivenDBInputFormat needs to interpolate between two values that
 * represent the lowest and highest valued records to import. Depending on the
 * data-type of the column, this requires different behavior. DBSplitter
 * implementations should perform this for a data type or family of data types.
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-1-7
 * @version v1.0
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface DBSplitter {
	/**
	 * Given a ResultSet containing one record (and already advanced to that
	 * record) with two columns (a low value, and a high value, both of the same
	 * type), determine a set of splits that span the given values.
	 * 
	 * @param conf 配置对象
	 * @param results 结果集
	 * @param colName 列名
	 * @return 拆分列表
	 * @throws SQLException SQL异常
	 */
	List<InputSplit> split(Configuration conf, ResultSet results, String colName) throws SQLException;
}
