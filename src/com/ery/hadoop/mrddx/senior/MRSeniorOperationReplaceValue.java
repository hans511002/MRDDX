package com.ery.hadoop.mrddx.senior;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import com.ery.hadoop.mrddx.DBRecord;

/**
 * 实现MR的高级应用接口类，替换数据 ; 结果为String类型
 * 语法规则：conreplace:{phone}:{123}-{456}:{789}-{10}
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wangpengkun
 * @createDate 2013-11-20
 * @version v1.0
 */
public class MRSeniorOperationReplaceValue implements IMRSeniorApply {

	private String replaceValStr;// 替换目标名称
	private String[] initKey; // 替换目标值
	private String[] initVal;
	private boolean status = true;

	public MRSeniorOperationReplaceValue(Configuration conf, int mapOrReduce) {
	}

	@Override
	public void apply(DBRecord dbRecord, Object obj) {
		if (null == dbRecord || dbRecord.isNull()) {
			return;
		}
		if (!this.status) {
			return;
		}
		Map<String, Object> row = dbRecord.getRow();
		if (!row.containsKey(this.replaceValStr)) {
			return;
		}

		String compareVal = (String)row.get(this.replaceValStr);
		for (int i = 0; i < this.initKey.length; i++) {
			if(null != this.initKey[i] && this.initKey[i].equals(compareVal)){
				row.put(this.replaceValStr, this.initVal[i]);
			}
		}
	}

	@Override
	public void initRule(String initStr) {
		if (null == initStr) {
			this.status = false;
			return;
		}

		String replaceValParam[] = initStr.split(":");
		if (replaceValParam.length <= 1) {
			return;
		}
		this.initKey = new String[replaceValParam.length-1];
		this.initVal = new String[replaceValParam.length-1];
		for (int i = 1; i < replaceValParam.length; i++) {
			String arrKeyVal[] = replaceValParam[i].split("-");
			if (arrKeyVal.length != 2) {
				continue;
			}
			this.initKey[i-1] = undock(arrKeyVal[0]);
			this.initVal[i-1] = undock(arrKeyVal[1]);
			
		//	this.initVal.put(undock(arrKeyVal[0]), undock(arrKeyVal[1]));
		}
		this.replaceValStr = undock(replaceValParam[0]);
	}

	private String undock(String kvarry) {
		if (null != kvarry && kvarry.startsWith("{") && kvarry.endsWith("}")) {
			return kvarry = kvarry.substring(1, kvarry.length() - 1);
		} else {
			return kvarry;
		}
	}

	public static void main(String[] args) {
		Map<String, Object> row = new HashMap<String, Object>();
		row.put("filepath", null);
		row.put("name", "fewfwe");
		row.put("pas", "filepath");
		row.put("A1", "202");

		DBRecord dbRecord = new DBRecord(row);
		MRSeniorOperationReplaceValue m = new MRSeniorOperationReplaceValue(
				null, 0);
		m.initRule("{A1}:{203}-{00001}:{202}-{00002}");
		long time = System.currentTimeMillis();
		for (int i = 0; i < 10000000; i++) {
			m.apply(dbRecord, "");
			if (i % 1000000 == 0) {
				System.out.println(dbRecord.getData("phone"));
			}
		}
		System.out.println("condition:" + (System.currentTimeMillis() - time));
		System.out.println(dbRecord.toString());

	}
}
