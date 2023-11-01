package io.auton8.spark.rule;

import static org.apache.spark.sql.functions.col;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import io.auton8.spark.exceptions.RuleNotApplicatbleException;

import static io.auton8.spark.utility.UtilityFunctions.normalizeColumnNameForDF;

public class CopyColumnRule implements IRule {

	@Override
	public boolean canApply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {
		if (params == null)
			throw new RuleNotApplicatbleException("Parameters cannot be null");
		if (!params.containsKey("originalColumn"))
			throw new RuleNotApplicatbleException("Original Column parameter is required");
		if (!params.containsKey("aliasColumn"))
			throw new RuleNotApplicatbleException("Alias Column parameter is required");
	
		return true;
	}

	@Override
	public Dataset<Row> apply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {
		if (canApply(df, params)) {

			String aliasColumn = (String) params.get("aliasColumn");
//			String originalColumn = (String) params.get("originalColumn");
			String copyColumn = (String) params.get("copyColumn");
			
			return df.withColumn(aliasColumn, col(normalizeColumnNameForDF(copyColumn)));
		}
		return null;
	}

}
