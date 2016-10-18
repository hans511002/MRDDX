package com.ery.hadoop.mrddx.remote.plugin;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.ery.hadoop.mrddx.file.FileSplit;
import com.ery.hadoop.mrddx.hbase.HbaseConfiguration;
import com.ery.base.support.utils.Convert;

public class DynaClass extends IRemotePlugin {
	FileSplit split;
	String priStr = "";
	String path = "";
	Expression expr = AviatorEvaluator.compile("cei+1", true);

	@Override
	public String line(String lineValue) throws IOException {
		String path = this.split.getPath().toString();
		if (!this.path.equals(path)) {
			this.path = path;
			priStr = path.substring(path.lastIndexOf("/") + 1).substring(0, 8) + ",";
		}
		return priStr + lineValue;
	}

	/**
	 * 返回空或0长度则抛弃当前记录
	 */
	@Override
	public List<Map<String, Object>> recode(final Map<String, Object> lineValue) throws IOException {
		Map<String, Object> nav = new HashMap<>();
		nav.put("cei", Convert.toInt(lineValue.get("cei"), 0));// convertToExecEnvObj(lineValue.get("cei").toString())
		Object o = expr.execute(nav);
		lineValue.put("cei", o.toString());
		return super.recode(lineValue);// 使用默认的List封装返回原数据
	}

	public static Object convertToExecEnvObj(String value) {
		try {
			if (value.indexOf('.') >= 0) {
				return Double.parseDouble(value);
			} else {
				return Long.parseLong(value);
			}
		} catch (NumberFormatException e) {
			return value;
		}
	}

	boolean getEexcBoolean(Object obj) throws IOException {
		boolean res = false;
		if (obj == null) {
			return res;
		} else if (obj instanceof Boolean) {
			res = (Boolean) obj;
		} else if (obj instanceof Integer) {
			res = ((Integer) obj > 0);
		} else if (obj instanceof Long) {
			res = ((Long) obj > 0);
		} else if (obj instanceof Double) {
			res = ((Double) obj > 0.000001);
		} else if (obj instanceof Float) {
			res = ((Float) obj > 0.000001);
		} else if (obj instanceof String) {
			res = obj.toString().equals("") || obj.toString().equals("null") || obj.toString().equals("NULL");
		}
		return res;
	}

	@Override
	public void configure(TaskInputOutputContext context) {
		org.apache.hadoop.mapreduce.Mapper.Context _context = (org.apache.hadoop.mapreduce.Mapper.Context) context;
		this.split = (FileSplit) _context.getInputSplit();
		// //DB
		// DBConfiguration conf = new
		// DBConfiguration(context.getConfiguration());
		// conf.getInputConnection()
		// conf.getOutputConnection()

		// ///////Hbase
		try {
			HbaseConfiguration hconf = new HbaseConfiguration(context.getConfiguration(), 0);
			HTable inht = new HTable(hconf.getConf(), "");
			HBaseConfiguration con = new HBaseConfiguration(hconf.getConf());
			HBaseAdmin a = new HBaseAdmin(con);
			Get get = new Get("".getBytes());
			get.addFamily("".getBytes());
			get.addColumn("".getBytes(), "".getBytes());
			Result r = inht.get(get);
			r.getRow();
			Cell c = r.getColumnLatestCell("".getBytes(), "".getBytes());
			new String(c.getValueArray(), c.getValueOffset(), c.getValueLength(), "UTF-8");
			// all version
			List<Cell> lcs = r.getColumnCells("".getBytes(), "".getBytes());

			hconf = new HbaseConfiguration(context.getConfiguration(), 1);
			HTable outht = new HTable(hconf.getConf(), "");
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void close() throws IOException {

	}

	public String toString() {
		return "Hello, I am " + this.getClass().getSimpleName();
	}
}