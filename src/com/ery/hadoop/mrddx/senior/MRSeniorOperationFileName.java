package com.ery.hadoop.mrddx.senior;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.file.FileSplit;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.StringUtil;

/**
 * 实现MR的高级应用接口类，获取文件名称的值或者某一部分值 输入字段的值可以从文件名取(按规则取值)-索引开始到结束或者按指定字符拆分后取某段
 * filename:{phone}:index[0-2] 索引开始到结束 ====================================
 * filename:{phone}:split{a}-{2} 指定字符拆分后取某段=================================
 * filename:reg:{DAY_NO}~reg~val 正则取值
 * 
 */
public class MRSeniorOperationFileName implements IMRSeniorApply {
	public static final Log LOG = LogFactory.getLog(MRSeniorOperationFileName.class);
	private String fieldName;
	private int type;
	private int[] type1Param = new int[2];
	private String type2SplitStr;
	private int type2SplitIndex;
	private boolean status = false;
	private Pattern pattern = null;
	String lastStr = null;
	String lastVal = null;

	public MRSeniorOperationFileName(Configuration conf, int mapOrReduce) {
	}

	private void initRegRule(String rule) {
		try {
			String ruleParam[] = rule.split("~");
			if (ruleParam.length != 3) {
				return;
			}
			this.fieldName = undock(ruleParam[0]);
			pattern = java.util.regex.Pattern.compile(ruleParam[1]);
			type2SplitStr = ruleParam[2];
		} catch (Exception e) {
			MRLog.errorException(LOG, "initRegRule:" + rule + " Exception", e);
		}
	}

	@Override
	public void initRule(String rule) {
		if (null == rule) {
			return;
		}
		if (rule.startsWith("reg:")) {
			this.type = 3;
			this.initRegRule(rule.substring(4));
			return;
		}
		String ruleParam[] = rule.split(":");
		if (ruleParam.length != 2 || ruleParam[0] == null || ruleParam[1] == null) {
			return;
		}

		this.fieldName = undock(ruleParam[0]);
		java.util.regex.Pattern pattern1 = java.util.regex.Pattern.compile("(\\d+)-(\\d+)");
		Matcher matcher1 = pattern1.matcher(ruleParam[1]);

		java.util.regex.Pattern pattern2 = java.util.regex.Pattern.compile("\\{(.*?)\\}-\\{(\\d+)\\}");
		Matcher matcher2 = pattern2.matcher(ruleParam[1]);
		if (ruleParam[1].startsWith("index")) {
			this.type = 1;
			if (matcher1.find()) {
				String s = matcher1.group(0);
				String tmp[] = s.split("-");
				if (tmp.length == 2) {
					this.type1Param[0] = StringUtil.stringToInt(tmp[0], -1);
					this.type1Param[1] = StringUtil.stringToInt(tmp[1], -1);
					// 验证
					if (-1 != this.type1Param[0] && -1 != this.type1Param[1] && type1Param[0] < type1Param[1]) {
						this.status = true;
					}
				}
			}
		} else if (ruleParam[1].startsWith("split")) {
			this.type = 2;
			if (matcher2.find()) {
				String s = matcher2.group(0);
				String tmp[] = s.split("-");
				if (tmp.length == 2) {
					this.type2SplitStr = this.undock(tmp[0]);
					this.type2SplitIndex = StringUtil.stringToInt(this.undock(tmp[1]), -1);
					// 验证
					if (this.type2SplitIndex != -1) {
						this.status = true;
					}
				}
			}
		}
	}

	private String undock(String kvarry) {
		if (null != kvarry && kvarry.startsWith("{") && kvarry.endsWith("}")) {
			return kvarry = kvarry.substring(1, kvarry.length() - 1);
		} else {
			MRLog.warn(LOG, "Execute_Replace: The format is not correct");
			return kvarry;
		}
	}

	@Override
	public void apply(DBRecord dbRecord, Object obj) {
		if (null == dbRecord || dbRecord.isNull()) {
			return;
		}

		if (!this.status) {
			return;
		}

		String rvalue = null;
		Map<String, Object> row = dbRecord.getRow();
		if (!row.containsKey(this.fieldName)) {
			return;
		}

		boolean isexit = false;
		if (null == obj) {
			return;
		}

		if (obj instanceof FileSplit) {
			rvalue = ((FileSplit) obj).getPath().toUri().toString();
			rvalue = rvalue.substring(rvalue.lastIndexOf("/") + 1);
			isexit = true;
		} else if (obj instanceof String) {
			rvalue = (String) obj;
			isexit = true;
		} else if (obj != null) {
			rvalue = obj.toString();
			isexit = true;
		}
		if (isexit) {
			if (rvalue.equals(this.lastStr)) {
				row.put(this.fieldName, this.lastVal);
				return;
			}
			this.lastStr = rvalue;
			switch (this.type) {
			case 1:
				if (rvalue.length() > this.type1Param[1]) {
					rvalue = rvalue.substring(this.type1Param[0], this.type1Param[1]);
					row.put(this.fieldName, rvalue);
				}
				break;
			case 2:
				String tmpVar[] = rvalue.split(this.type2SplitStr);
				if (this.type2SplitIndex < tmpVar.length) {
					rvalue = tmpVar[this.type2SplitIndex];
					row.put(this.fieldName, rvalue);
				}
				break;
			case 3:
				Matcher m = pattern.matcher(rvalue);
				if (m.find()) {
					rvalue = m.replaceAll(type2SplitStr);
					row.put(this.fieldName, rvalue);
				}
				break;
			default:
				break;
			}
			this.lastVal = rvalue;
		}
	}

	public static void main(String[] args) {
		Map<String, Object> row = new HashMap<String, Object>();
		row.put("filepath", null);
		row.put("name", "fewfwe");
		row.put("pas", "filepath");
		row.put("phone", "asdf#23");

		DBRecord dbRecord = new DBRecord(row);
		MRSeniorOperationFileName m = new MRSeniorOperationFileName(null, 0);
		m.initRule("{phone}:index[0-3]");
		// m.initRule("{phone}:split{\\.}-{1}");

		long time = System.currentTimeMillis();
		for (int i = 0; i < 1; i++) {
			m.apply(dbRecord, "hive_txt.txt");
			if (i % 1000000 == 0) {
				System.out.println(dbRecord.getData("phone"));
			}
		}
		System.out.println("condition:" + (System.currentTimeMillis() - time));
		System.out.println(dbRecord.toString());

		// java.util.regex.Pattern pattern1 =
		// java.util.regex.Pattern.compile("\\{(.*?)\\}-\\{(\\d+)\\}");
		// Matcher matcher1 = pattern1.matcher("split{qwqs./a.s/a}-{2}");
		// while (matcher1.find()) {
		// System.out.println(matcher1.group(0));
		// }
	}
}
