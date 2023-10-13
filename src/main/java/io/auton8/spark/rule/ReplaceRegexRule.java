package io.auton8.spark.rule;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_replace;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import io.auton8.spark.exceptions.RuleNotApplicatbleException;

public class ReplaceRegexRule implements IRule {
	
	public static Dataset<Row> replaceRegex(Dataset<Row> df, String originalColumn, String aliasColumn, String findReg, String replaceValue) {
		return df.withColumn(aliasColumn, regexp_replace(col(originalColumn), findReg, replaceValue));
	}

	@Override
	public boolean canApply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {
		if (params == null)
			throw new RuleNotApplicatbleException("Parameters cannot be null");
		if (!params.containsKey("originalColumn"))
			throw new RuleNotApplicatbleException("Original Column parameter is required");
		if (!params.containsKey("aliasColumn"))
			throw new RuleNotApplicatbleException("Alias Column parameter is required");
		if (!params.containsKey("findReg"))
			throw new RuleNotApplicatbleException("findReg value parameter is required");
		if (!params.containsKey("replaceValue"))
			throw new RuleNotApplicatbleException("replaceValue  parameter is required");
		return true;
	}

	@Override
	public Dataset<Row> apply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {
		if (canApply(df, params)) {

			String aliasColumn = (String) params.get("aliasColumn");
			String originalColumn = (String) params.get("originalColumn");
			String findReg = (String) params.get("findReg");
			String replaceValue = (String) params.get("replaceValue");
			
			return df.withColumn(aliasColumn, regexp_replace(col(originalColumn), findReg, replaceValue));
		}
		return df;
	}

}
