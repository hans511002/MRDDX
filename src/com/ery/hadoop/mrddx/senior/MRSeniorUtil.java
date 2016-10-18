package com.ery.hadoop.mrddx.senior;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.ery.hadoop.mrddx.MRConfiguration;

/**
 * 高级应用工具类
 * 
 * @copyRights @ 2012-2013,Tianyuan DIC Information Co.,Ltd. All rights
 *             reserved.
 * @author wanghao
 * @createDate 2013-2-18
 * @version v1.0
 */
public class MRSeniorUtil {
	
	/**
	 * 获取高级应用对象列表
	 * 
	 * @param sconf
	 *            规则
	 * @param conf 
	 * @param isPatternFlag 
	 * @return 应用对象列表
	 */
	public static IMRSeniorApply[] getMRSeniorApplies(String sconf, Configuration conf, int mapOrReduce) {
		// 初始化Senior对象
		String oper[] = sconf.split(",");
		IMRSeniorApply mrSeniorApply[] = new IMRSeniorApply[oper.length];
		for (int i = 0; i < oper.length; i++) {
			String ch = MRSeniorUtil.startOperation(oper[i]);
			IMRSeniorApply apply = MRSeniorUtil.getMRSeniorApply(ch, conf, mapOrReduce);
			if (null != apply) {
				mrSeniorApply[i] = apply;
				if (null != ch && oper[i].length() > ch.trim().length()) {
					apply.initRule(oper[i].substring(oper[i].indexOf(":")+1, oper[i].length()));
				}
			}
		}

		return mrSeniorApply;
	}

	/**
	 * 获取高级应用满足条件的规则
	 * 
	 * @param sconf
	 *            规则
	 * @return 规则数组
	 */
	public static String[] getMRSeniorApplyRules(String sconf) {
		// 初始化Senior对象
		String oper[] = sconf.split(",");
		String[] mrSeniorApplyRule = new String[oper.length];
		for (int i = 0; i < oper.length; i++) {
			String ch = MRSeniorUtil.startOperation(oper[i]);
			if (null != ch && oper[i].length() > ch.trim().length()) {
				mrSeniorApplyRule[i] = oper[i].substring(oper[i].indexOf(":")+1, oper[i].length());
			}
		}

		return mrSeniorApplyRule;
	}

	/**
	 * 获取高级应用对象
	 * 
	 * @param oper
	 *            应用类型
	 * @param mapOrReduce 0:map, 1:reduce 
	 * @return 应用对象
	 */
	public static IMRSeniorApply getMRSeniorApply(String oper, Configuration conf, int mapOrReduce) {
		if (null == oper || oper.length() <= 0) {
			return null;
		}

		if (IMRSeniorApply.TYPE_OPERATION_DIGIT.equals(oper)) {
			return new MRSeniorOperationDigit(conf, mapOrReduce);
		}else if (IMRSeniorApply.TYPE_OPERATION_FILTER.equals(oper)) {
			return new MRSeniorOperationFilter(conf, mapOrReduce);
		}else if (IMRSeniorApply.TYPE_OPERATION_REPLACE.equals(oper)) {
			return new MRSeniorOperationReplace(conf, mapOrReduce);
		}else if (IMRSeniorApply.TYPE_OPERATION_FILENAME.equals(oper)) {
			return new MRSeniorOperationFileName(conf, mapOrReduce);
		}else if (IMRSeniorApply.TYPE_OPERATION_REVERSE.equals(oper)) {
			return new MRSeniorOperationReverse(conf, mapOrReduce);
		}else if (IMRSeniorApply.TYPE_OPERATION_CONDITION_REPLACE.equals(oper)) {
			return new MRSeniorOperationReplaceValue(conf, mapOrReduce);
	}

		return null;
	}

	/**
	 * 获取高级应用对象应用类型
	 * 
	 * @param oper 操作
	 * @return String: 类型名称
	 */
	public static String startOperation(String oper) {
		if (null == oper || oper.length() <= 0) {
			return null;
		}

		if (oper.startsWith(IMRSeniorApply.TYPE_OPERATION_DIGIT+":")) {
			return IMRSeniorApply.TYPE_OPERATION_DIGIT;
		}
		
		if (oper.startsWith(IMRSeniorApply.TYPE_OPERATION_FILTER+":")) {
			return IMRSeniorApply.TYPE_OPERATION_FILTER;
		}
		
		if (oper.startsWith(IMRSeniorApply.TYPE_OPERATION_REPLACE+":")) {
			return IMRSeniorApply.TYPE_OPERATION_REPLACE;
		}

		if (oper.startsWith(IMRSeniorApply.TYPE_OPERATION_FILENAME+":")) {
			return IMRSeniorApply.TYPE_OPERATION_FILENAME;
		}
		
		if (oper.startsWith(IMRSeniorApply.TYPE_OPERATION_REVERSE+":")) {
			return IMRSeniorApply.TYPE_OPERATION_REVERSE;
		}

		if (oper.startsWith(IMRSeniorApply.TYPE_OPERATION_CONDITION_REPLACE+":")) {
			return IMRSeniorApply.TYPE_OPERATION_CONDITION_REPLACE;
		}

		return null;
	}
	
	/**
	 * 获取过滤数据存放的根路径
	 * @param conf
	 */
	public static String getFilterRootPath(MRConfiguration conf, IMRSeniorApply mrSeniorApply[]) {
		if (null == mrSeniorApply || mrSeniorApply.length == 0){
			return null;
		}
		String outputPath = null;
		boolean flag = false;
		for (int j = 0; j < mrSeniorApply.length; j++) {
			if (null != mrSeniorApply[j] || mrSeniorApply[j] instanceof MRSeniorOperationFilter) {
				flag = true;
				break; 
			}
		}
		
		if (!flag){
			return null;
		}
		
		try {
    		if (conf.isOutputMapSeniorFilterIsOutput()){
    			outputPath = conf.getOutputMapSeniorFilterOutputPath();
    			if (null != outputPath){
    				FileSystem fs = FileSystem.get(conf.getConf());
    				Path path = new Path(outputPath);
    				if (!fs.exists(path)){
    					fs.mkdirs(path);
    					return outputPath;
    				}else{
    					if(fs.isFile(path)){
    						return conf.getMapredInputDir();
    					}else if (fs.getFileStatus(path).isDir()){
    						return outputPath;
    					}
    				}
    			}
    		}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return outputPath;
	}
}
