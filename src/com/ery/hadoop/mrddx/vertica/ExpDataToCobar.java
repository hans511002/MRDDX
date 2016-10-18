package com.ery.hadoop.mrddx.vertica;

import java.sql.ResultSet;

import com.ery.base.support.jdbc.DataAccess;
import com.ery.base.support.jdbc.DataSourceImpl;

public class ExpDataToCobar {

	public static void main(String[] args) throws Exception {
		String selSql = "select to_char(START_TIME,'yyyymm') MONTH_NO"
				+ ",to_char(START_TIME,'yyyymmdd') DAY_NO,S_WEEK WEEK_NO,"
				+ "F0002 IMSI,PROVINCE_ID,CITY_ID,DISTRICT_ID,VENDOR_ID,VERSION_ID,RELATED_OMC,"
				+ "RELATED_BSC,RELATED_BTS,RELATED_SECTOR,GEO_AREA,grid_row,grid_column,RADIAL_DISTANCE"
				+ ",START_TIME,WEEK_DAY,INSERT_TIME,F0001,F0003,F0004,F0005,F0006,F0007,F0008,F0009,F0010,F0011"
				+ ",F0012,F0013,F0014,F0015,F0016,F0017,F0018,F0019,F0020,F0021,F0022,F0023,F0024,F0025,F0026"
				+ ",F0027,F0028,F0029,F0030,F0031,F0032,F0033,F0034,F0035,F0036,longitude_left_up,latitude_left_up"
				+ ",longitude_right_down,latitude_right_down from CDL_CDT_1X_PP_BASIC  limit 1" + " ";// where
		// to_char(start_time,'yyyymmdd')
		// vertica
		DataSourceImpl vds = new DataSourceImpl("com.vertica.jdbc.Driver", "jdbc:vertica://133.37.31.150:5433/bdap",
				"dbadmin", "bdap");
		DataAccess vad = new DataAccess(vds.getConnection());

		// String day = "20150101";
		// String endDay = "20150101";
		// if (args.length > 0) {
		// day = args[0];
		// }
		// if (args.length > 1) {
		// endDay = args[1];
		// }
		// if (day.equals(endDay) || Integer.parseInt(day) >
		// Integer.parseInt(endDay)) {
		// selSql += " ='" + day + "' ";
		// } else {
		// selSql += "in (";
		// int s = Integer.parseInt(day);
		// int e = Integer.parseInt(endDay);
		// Date st = new Date();
		// st.setYear(s / 10000 - 1900);
		// st.setMonth((s / 100) % 100 - 1);
		// st.setDate(s % 100);
		//
		// Date et = new Date();
		// st.setYear(e / 10000 - 1900);
		// st.setMonth((e / 100) % 100 - 1);
		// st.setDate(e % 100);
		// for (int i = s; i <= e; i++) {
		// selSql += "'" + i + "',";
		// }
		// selSql = selSql.substring(0, selSql.length() - 1);
		// selSql += ")";
		// }
		// ob
		DataSourceImpl ods = new DataSourceImpl("jdbc:mysql://133.37.31.51:8066/net", "net", "net");
		DataAccess oad = new DataAccess(ods.getConnection());
		String inertSql = "replace into CDL_CDT_1X_PP_BASIC(MONTH_NO,DAY_NO,WEEK_NO,IMSI,PROVINCE_ID,CITY_ID,DISTRICT_ID,VENDOR_ID,VERSION_ID,RELATED_OMC,RELATED_BSC,RELATED_BTS,RELATED_SECTOR,GEO_AREA,grid_row,grid_column,RADIAL_DISTANCE,START_TIME,WEEK_DAY,INSERT_TIME,F0001,F0003,F0004,F0005,F0006,F0007,F0008,F0009,F0010,F0011,F0012,F0013,F0014,F0015,F0016,F0017,F0018,F0019,F0020,F0021,F0022,F0023,F0024,F0025,F0026,F0027,F0028,F0029,F0030,F0031,F0032,F0033,F0034,F0035,F0036,longitude_left_up,latitude_left_up,longitude_right_down,latitude_right_down)";
		inertSql += "values(";
		System.out.println("query sql=" + selSql);
		final ResultSet rs = vad.execQuerySql(selSql);
		int rowCount = 0;
		int colCount = rs.getMetaData().getColumnCount();
		for (int i = 0; i < colCount; i++) {
			inertSql += "?";
			if (i < colCount - 1)
				inertSql += ",";
		}
		inertSql += ")";
		int blen = 3000;
		Object[][] params = new Object[blen][];
		oad.beginTransaction();
		oad.setQueryTimeout(600);
		while (rs.next()) {
			int index = rowCount % blen;
			rowCount++;
			params[index] = new Object[colCount];
			for (int i = 0; i < colCount; i++) {
				params[index][i] = rs.getObject(i + 1);
			}
			// oad.execUpdate(inertSql, params[index]);
			if (index == blen - 1) {
				oad.execUpdateBatch(inertSql, params);
				oad.commit();
				oad.beginTransaction();
			}
			if ((rowCount % 100000) == 0) {
				System.out.println(rowCount + " rows");
			}
		}
		oad.commit();
		System.out.println(rowCount + " rows");
	}
}
