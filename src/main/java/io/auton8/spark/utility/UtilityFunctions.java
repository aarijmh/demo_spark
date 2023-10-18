package io.auton8.spark.utility;

public class UtilityFunctions {
	public static String normalizeColumnNameForDF(String columnName) {
		if (columnName.contains("."))
			return "`" + columnName + "`";
		return columnName;
	}
}
