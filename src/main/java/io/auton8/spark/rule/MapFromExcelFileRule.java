package io.auton8.spark.rule;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import io.auton8.spark.exceptions.RuleNotApplicatbleException;
import io.auton8.spark.utility.ExcelReader;
import io.auton8.spark.utility.UtilityFunctions;

public class MapFromExcelFileRule implements IRule {

	@Override
	public boolean canApply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {

		if (params == null)
			throw new RuleNotApplicatbleException("Parameters cannot be null");
		if (!params.containsKey("excelSheetPath"))
			throw new RuleNotApplicatbleException("excelSheetPath parameter is required");
		if (!params.containsKey("sheetName"))
			throw new RuleNotApplicatbleException("sheetName parameter is required");
		if (!params.containsKey("keyColumn"))
			throw new RuleNotApplicatbleException("keyColumn parameter is required");
		if (!params.containsKey("valueColumn"))
			throw new RuleNotApplicatbleException("valueColumn parameter is required");
		if (!params.containsKey("joinColumn"))
			throw new RuleNotApplicatbleException("joinColumn parameter is required");
		if (!params.containsKey("aliasColumn"))
			throw new RuleNotApplicatbleException("Alias Column parameter is required");
		if (!params.containsKey("sparkSession"))
			throw new RuleNotApplicatbleException("sparkSession parameter is required");
		try {
			if (!((SparkSession) params.get("sparkSession") instanceof SparkSession)) {

				throw new RuleNotApplicatbleException("sparkSession parameter is invalid");
			}
		} catch (Exception e) {
			throw new RuleNotApplicatbleException("sparkSession parameter is invalid");
		}
		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Dataset<Row> apply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {
		if (canApply(df, params)) {

			String aliasColumn = (String) params.get("aliasColumn");
			String excelSheetPath = (String) params.get("excelSheetPath");
			String sheetName = (String) params.get("sheetName");
			String keyColumn = (String) params.get("keyColumn");
			String valueColumn = (String) params.get("valueColumn");
			String joinColumn = (String) params.get("joinColumn");
			List<String> dropTargetColumns = params.containsKey("dropTargetColumns") ? (List<String>) params.get("dropTargetColumns") : new ArrayList<String>();

			SparkSession sparkSession = (SparkSession) params.get("sparkSession");

			ExcelReader excelReader = new ExcelReader();

			Dataset<Row> sectorDF = null;
			try {
				sectorDF = excelReader.readDatasetFromExcel(sparkSession, Path.of(excelSheetPath), sheetName, keyColumn,
						valueColumn).select(UtilityFunctions.normalizeColumnNameForDF(keyColumn), UtilityFunctions.normalizeColumnNameForDF(valueColumn));
				for(String col : dropTargetColumns) {
					sectorDF = sectorDF.drop(UtilityFunctions.normalizeColumnNameForDF(col));
				}
			} catch (Exception e) {
				throw new RuleNotApplicatbleException(e.getMessage());
			}

			boolean found = false;
			boolean foundKey = false;

			for (String columnName : df.columns()) {
				if (columnName.equals(aliasColumn)) {
					found = true;
				}
				if (columnName.equals(keyColumn)) {
					foundKey = true;
				}

				if (found && foundKey)
					break;
			}

			if (foundKey) {
				sectorDF = sectorDF.withColumnRenamed(keyColumn, keyColumn + "1");
				keyColumn += "1";
			}

			if (found) {
				df = df.drop (aliasColumn);
			}
			
			df = df.join(sectorDF, sectorDF.col(keyColumn).eqNullSafe(df.col(joinColumn)), "left");
			
			df = df.withColumnRenamed(valueColumn, aliasColumn);
			//df.show(10);
			return df;
		}
		return df;
	}

}
