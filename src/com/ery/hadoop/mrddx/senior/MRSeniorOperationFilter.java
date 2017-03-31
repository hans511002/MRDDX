package com.ery.hadoop.mrddx.senior;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.RuleConvert;

/**
 * 实现MR的高级应用接口类，处理运算:+ - * / ; 结果为浮点类型
 * rule例子：filter:{phone}:[^0-9+]



 * @createDate 2013-2-18
 * @version v1.0
 */
public class MRSeniorOperationFilter implements IMRSeniorApply {
	public static final Log LOG = LogFactory.getLog(MRSeniorOperationFilter.class);
	private Pattern pattern;
	private String patternRule;
	private String expression;
	// 默认为true, true表示保留匹配正则表达式的数据, false表示不保留匹配正则表达式的数据
	private boolean isPatternFlag = true;
	
	public MRSeniorOperationFilter(Configuration conf, int mapOrReduce) {
		MRConfiguration mrconf = new MRConfiguration(conf);
		switch (mapOrReduce) {
		case 0:
			this.isPatternFlag = mrconf.isInputMapSeniorFilterFlag();
			break;
		case 1:
			this.isPatternFlag = mrconf.isOutputMapSeniorFilterFlag();
			break;
		default:
			break;
		}
	}
	
	@Override
	public void initRule(String rule) {
		String tmp[] = rule.split(":");
		if (tmp.length == 2){
			this.expression=tmp[0];
			this.patternRule=tmp[1];
			try {
				this.pattern = Pattern.compile(this.patternRule);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public void apply(DBRecord dbRecord, Object obj) {
		if (null == dbRecord || dbRecord.isNull() || null == this.pattern) {
			return;
		}
		
		if (null == this.expression && this.expression.trim().length()<=0){
			return;
		}

		// 计算语句
		RuleConvert ruleCovert = new RuleConvert(this.expression);
		String opera = ruleCovert.getValue(dbRecord.getRow());
		if (this.expression.equals(opera)){
			return;
		}
		
		try {
			if(this.isPatternFlag){
				dbRecord.setStatus(this.pattern.matcher(opera).matches());
			}else{
				dbRecord.setStatus(!this.pattern.matcher(opera).matches());
			}
		} catch (Exception e) {
			MRLog.warnException(LOG, "Execute_Filter:" + opera, e);
		}
	}
	
	public static void main(String[] args) {
		Map<String, Object> row = new HashMap<String, Object>();
		row.put("name", "fewfwe");
		row.put("pas", "filepath");
		row.put("phone", "120131");

		DBRecord dbRecord = new DBRecord(row);
		MRSeniorOperationFilter m = new MRSeniorOperationFilter(null, 0);
		m.initRule("{phone}:[1-9]+");
		m.apply(dbRecord, null);
		System.out.println(dbRecord.isStatus());
		System.out.println(dbRecord.toString());
	}
}
