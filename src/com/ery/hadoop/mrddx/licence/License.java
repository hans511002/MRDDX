package com.ery.hadoop.mrddx.licence;

import license.CheckLicense;

//import org.apache.jra.btsgg.CheckLicense;

/**
 * @version v1.0
 */
public class License {
	public static void checkLicense() {
		CheckLicense.setAppName("HADOOP");
		try {
			if (!CheckLicense.checkLicense()) {
				throw new RuntimeException("license check failed, please update license!");
			}
		} catch (Exception e) {
			throw new RuntimeException("license check failed, please update license!");
		}
	}
}
