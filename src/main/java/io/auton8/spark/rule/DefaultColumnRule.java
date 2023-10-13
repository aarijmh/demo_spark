package io.auton8.spark.rule;

import static org.apache.spark.sql.functions.lit;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import io.auton8.spark.exceptions.RuleNotApplicatbleException;

public class DefaultColumnRule implements IRule {

	public static Dataset<Row> createDefaultColumn(Dataset<Row> df, String aliasColumn, String defaultValue) {
		return df.withColumn(aliasColumn, lit(defaultValue));
	}
	
	@Override
	public boolean canApply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {
		if (params == null)
			throw new RuleNotApplicatbleException("Parameters cannot be null");
		if (!params.containsKey("aliasColumn"))
			throw new RuleNotApplicatbleException("Alias Column parameter is required");
		if (!params.containsKey("defaultValue"))
			throw new RuleNotApplicatbleException("defaultValue value parameter is required");
		return true;
	}

	@Override
	public Dataset<Row> apply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {
		if (canApply(df, params)) {

			String aliasColumn = (String) params.get("aliasColumn");
			String defaultValue = (String) params.get("defaultValue");
			return df.withColumn(aliasColumn, lit(defaultValue));
		}
		return null;
	}

}
