package com.ery.hadoop.mrddx.remote.plugin;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapred.MapTask;
import org.apache.hadoop.mapred.ReduceTask;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.mapreduce.task.TaskInputOutputContextImpl;

import com.ery.hadoop.mrddx.DBRecord;
import com.ery.hadoop.mrddx.MRConfiguration;
import com.ery.hadoop.mrddx.log.MRLog;
import com.ery.hadoop.mrddx.util.StringUtil;

/**
 * 插件接口
 * 
 */
public abstract class IRemotePlugin {
	protected List<Map<String, Object>> res = new ArrayList<Map<String, Object>>();
	protected TaskInputOutputContext context;
	protected Mapper map;
	protected Reducer reducer;

	public static void main(String[] args) throws Exception {
		Class<?> class1 = getClassSubCls(MapTask.class, "NewTrackingRecordReader");
		System.out.println(class1);
	}

	public static RecordWriter getRealOutPutStream(TaskInputOutputContext context) throws IOException {
		System.err.println("context className:" + context.getClass().getCanonicalName());
		if (context instanceof WrappedMapper.Context) {
			try {
				Field mapFd = getClassField(WrappedMapper.Context.class, "mapContext");
				mapFd.setAccessible(true);
				Object ct = mapFd.get(context);
				if (ct == null) {
					throw new IOException("WrappedMapper.Context   field mapContext value is null");
				}
				if (ct instanceof TaskInputOutputContextImpl) {
					Field field = getClassField(TaskInputOutputContextImpl.class, "output");
					System.err.println("MapContextImpl Field :" + field.getType().getCanonicalName() + " " +
							field.getName());
					field.setAccessible(true);
					RecordWriter r = (RecordWriter) field.get(ct);
					System.err.println("RecordWriter className:" + r.getClass().getCanonicalName());
					Class<?> class1 = getClassSubCls(MapTask.class, "NewDirectOutputCollector");
					if (class1.isInstance(r)) {
						field = getClassField(class1, "out");
						System.err.println("NewDirectOutputCollector out Field  :" +
								field.getType().getCanonicalName() + " " + field.getName());
						field.setAccessible(true);
						return (RecordWriter) field.get(r);
					}
				}
			} catch (Exception e) {
				e.printStackTrace(System.err);
				throw new IOException(e);
			}
		} else if (context instanceof WrappedReducer.Context) {
			try {
				Field mapFd = getClassField(WrappedReducer.Context.class, "reduceContext");
				mapFd.setAccessible(true);
				Object ct = mapFd.get(context);
				if (ct == null) {
					throw new IOException("WrappedReducer.Context   field reduceContext value is null");
				}
				if (ct instanceof TaskInputOutputContextImpl) {
					Field field = getClassField(TaskInputOutputContextImpl.class, "output");
					System.err.println("TaskInputOutputContextImpl Field :" + field.getType().getCanonicalName() + " " +
							field.getName());
					field.setAccessible(true);
					RecordWriter r = (RecordWriter) field.get(ct);
					System.err.println("RecordWriter className:" + r.getClass().getCanonicalName());
					Class<?> class1 = getClassSubCls(ReduceTask.class, "NewTrackingRecordWriter");
					if (class1.isInstance(r)) {
						field = getClassField(class1, "real");
						System.err.println("NewTrackingRecordWriter out Field  :" + field.getType().getCanonicalName() +
								" " + field.getName());
						field.setAccessible(true);
						return (RecordWriter) field.get(r);
					}
				}
			} catch (Exception e) {
				e.printStackTrace(System.err);
				throw new IOException(e);
			}
		}
		return null;
	}

	public static Field getClassField(Class<?> clz, String fieldName) {
		Field[] fds = clz.getDeclaredFields();
		for (Field field : fds) {
			if (field.getName().equals(fieldName)) {
				return field;
			}
		}
		return null;
	}

	public static Class<?> getClassSubCls(Class<?> clz, String className) {
		Class<?>[] subcs = clz.getDeclaredClasses();
		for (Class<?> class1 : subcs) {
			if (class1.getName().endsWith("$" + className)) {
				return class1;
			}
		}
		return null;
	}

	public static RecordReader getRealInputStream(TaskInputOutputContext context) throws IOException {
		System.err.println("context className:" + context.getClass().getCanonicalName());
		if (context instanceof WrappedMapper.Context) {
			try {
				Field mapFd = getClassField(WrappedMapper.Context.class, "mapContext");
				mapFd.setAccessible(true);
				Object ct = mapFd.get(context);
				if (ct == null) {
					throw new IOException("WrappedMapper.Context   field mapContext value is null");
				}
				if (ct instanceof MapContextImpl) {
					Field field = getClassField(MapContextImpl.class, "reader");
					System.err.println("MapContextImpl Field :" + field.getType().getCanonicalName() + " " +
							field.getName());
					field.setAccessible(true);
					RecordReader r = (RecordReader) field.get(ct);
					System.err.println("RecordReader className:" + r.getClass().getCanonicalName());
					Class<?> class1 = getClassSubCls(MapTask.class, "NewTrackingRecordReader");
					if (class1.isInstance(r)) {
						field = getClassField(class1, "real");
						System.err.println("NewTrackingRecordReader real Field  :" +
								field.getType().getCanonicalName() + " " + field.getName());
						field.setAccessible(true);
						return (RecordReader) field.get(r);
					}
				}
			} catch (Exception e) {
				e.printStackTrace(System.err);
				throw new IOException(e);
			}
		}
		return null;
	}

	public static List<Map<String, Object>> mapPlugin(IRemotePlugin remotePlugin, Map<String, Object> value)
			throws IOException {
		if (remotePlugin != null) {// 插件处理
			return remotePlugin.recode(value);
		}
		List<Map<String, Object>> res = new ArrayList<Map<String, Object>>();
		res.add(value);
		return res;
	}

	public static String line(IRemotePlugin remotePlugin, String value) throws IOException {
		if (remotePlugin != null) {// 插件处理
			return remotePlugin.line(value);
		}
		return value;
	}

	public static IRemotePlugin configurePlugin(TaskInputOutputContext context, Object mapReducer) {
		MRConfiguration mrconf = new MRConfiguration(context.getConfiguration());

		String className = "";
		String code = "";
		if (context instanceof Mapper.Context) {
			className = mrconf.getInputMapPluginClassName();
			code = mrconf.getInputMapPluginCode();
		} else if (context instanceof Reducer.Context) {
			className = mrconf.getOutputMapPluginClassName();
			code = mrconf.getOutputMapPluginCode();
		}
		if (null != className && className.trim().length() > 0 && null != code && code.trim().length() > 0) {
			try {
				MRLog.systemOut("开始初始化插件");
				long start = System.currentTimeMillis();
				IRemotePlugin remotePlugin = DynaManager.INSTANCE.getPluginObject(className, code);
				long end = System.currentTimeMillis();
				MRLog.systemOut("成功初始化插件, 耗时:" + (end - start) + "ms");
				if (context instanceof Mapper.Context) {
					remotePlugin.map = (Mapper) mapReducer;
				} else if (context instanceof Reducer.Context) {
					remotePlugin.reducer = (Reducer) mapReducer;
				}
				remotePlugin.configure(context);
				return remotePlugin;
			} catch (Exception e) {
				String msg = "初始化插件失败:" + StringUtil.printStackTrace(e);
				MRLog.getInstance().jobMapRunMsg(mrconf.getTaskID(), MRLog.LOG_TYPE_ERROR, new Date(), msg);
				throw new RuntimeException(msg);
			}
		}
		return null;
	}

	public static void closePlugin(IRemotePlugin remotePlugin) throws IOException {
		if (null != remotePlugin) {
			remotePlugin.close();
		}
	}

	public static void beforConvertRow(IRemotePlugin remotePlugin, DBRecord row) {
		if (remotePlugin != null) {// 插件处理
			remotePlugin.beforConvertRow(row);
		}
	}

	/**
	 * MAP 适用于拆分字段后的处理
	 * 
	 * @param recode
	 *            记录
	 * @return false表示需被过滤掉的数据
	 * @throws IOException
	 */
	public List<Map<String, Object>> recode(Map<String, Object> recode) throws IOException {
		res.clear();
		res.add(recode);
		return res;
	};

	/**
	 * 采集文本文件中或者MAP读取文本文件（适用于按行读取的处理）
	 * 
	 * @param lineValue
	 *            行记录
	 * @return 返回处理过后的行记录值，返回null表示过滤掉该记录
	 * @throws IOException
	 */
	public String line(String lineValue) throws IOException {
		return lineValue;
	};

	/**
	 * 配置相关内容
	 * 
	 * @param job
	 */
	public void configure(TaskInputOutputContext context) throws Exception {
		this.context = context;
	};

	public void beforConvertRow(DBRecord row) {

	}

	/**
	 * 关闭资源
	 * 
	 * @throws IOException
	 */
	public void close() throws IOException {
	};
}