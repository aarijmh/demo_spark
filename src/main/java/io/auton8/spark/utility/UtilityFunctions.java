package io.auton8.spark.utility;

import static org.apache.spark.sql.functions.when;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class UtilityFunctions {
	public static String normalizeColumnNameForDF(String columnName) {
		if (columnName.contains("`")) {
			return columnName;
		}
		if (columnName.contains("."))
			return "`" + columnName + "`";
		return columnName;
	}
	
	
	public static  Dataset<Row> compareColumns(Dataset<Row> df, String compColumn, Column originalColumn, Column targetColumn, String matchedString, String notMatchedString){
		return df.withColumn(compColumn, when(originalColumn.isNull().and(targetColumn.isNull()),matchedString)
				.otherwise(when(originalColumn.isNull().and(targetColumn.eqNullSafe("")),matchedString)
						.otherwise(when(originalColumn.isNull().and(targetColumn.eqNullSafe("")),matchedString)
								.otherwise(when(originalColumn.eqNullSafe("").and(targetColumn.isNull()),matchedString)
										.otherwise(when(originalColumn.eqNullSafe(targetColumn),matchedString).otherwise(notMatchedString))))));
				
	}
}
