package com.ery.hadoop.mrddx.senior;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.file.FileSplit;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.StringUtil;

/**
 * 实现MR的高级应用接口类，字段值替换功能 语法： 
 * replace:{phone}:{filepath}
 * 



 * @createDate 2013-2-18
 * @version v1.0
 */
public class MRSeniorOperationReplace implements IMRSeniorApply {
	public static final Log LOG = LogFactory.getLog(MRSeniorOperationReplace.class);
	private String fieldName;
	private boolean status = true; // 解析规则状态，true：正确， false：错误

	public MRSeniorOperationReplace(Configuration conf, int mapOrReduce) {
	}

	@Override
	public void initRule(String rule) {
		String msg = "";
		this.fieldName = StringUtil.decodeString(rule, "{", "}");
		if (null == this.fieldName){
			this.status = false;
		}
		
		if (!this.status){
			MRLog.warn(LOG, "Execute_Replace: status error, "+msg);
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

		if (obj instanceof FileSplit) {
			rvalue = ((FileSplit) obj).getPath().toUri().toString();
			row.put(this.fieldName, rvalue);
		} else if (obj instanceof String) {
			rvalue = (String) obj;
			row.put(this.fieldName, rvalue);
		}
	}

	public static void main(String[] args) {
		Map<String, Object> row = new HashMap<String, Object>();
//		row.put("filepath", null);
		row.put("name", "fewfwe");
		row.put("pas", "filepath");
		row.put("phone", "asdf#23");

		DBRecord dbRecord = new DBRecord(row);
		MRSeniorOperationReplace m = new MRSeniorOperationReplace(null, 0);
		m.initRule("{phone}");
//		m.initRule("replace:{pas}:{1232}-{1}:{filepath}-{filepath}");
		long time = System.currentTimeMillis();
		for (int i = 0; i < 1; i++) {
			m.apply(dbRecord, "/haome/fadsf/");
			if (i % 1000000 == 0) {
				System.out.println(dbRecord.getData("phone"));
			}
		}
		System.out.println("condition:" + (System.currentTimeMillis() - time));
		System.out.println(dbRecord.toString());
	}
}
