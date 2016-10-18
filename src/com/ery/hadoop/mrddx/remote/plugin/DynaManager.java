package com.ery.hadoop.mrddx.remote.plugin;


public class DynaManager {
	public static DynaManager INSTANCE = new DynaManager();
	private DynaManager(){
		
	}
	
	public IRemotePlugin getPluginObject(String fullName, String code) throws Exception{
		DynamicEngine de = DynamicEngine.getInstance();
		Object obj = null;
		fullName = "com.ery.hadoop.mrddx.remote.plugin."+fullName;
		try {
			obj = de.javaCodeToObject(fullName, code.toString());
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		}
		
		if (!(obj instanceof IRemotePlugin)){
			throw new Exception("");
		}
		return (IRemotePlugin)obj;
	}
}
