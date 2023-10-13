package io.auton8.spark.rule;

import static org.apache.spark.sql.functions.expr;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import io.auton8.spark.exceptions.RuleNotApplicatbleException;

public class MapValuesFromColumnRule implements IRule {

	@SuppressWarnings("unchecked")
	@Override
	public boolean canApply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {
		if (params == null)
			throw new RuleNotApplicatbleException("Parameters cannot be null");
		if (!params.containsKey("originalColumn"))
			throw new RuleNotApplicatbleException("Original Column parameter is required");
		if (!params.containsKey("aliasColumn"))
			throw new RuleNotApplicatbleException("Alias Column parameter is required");
		if (!params.containsKey("valueMap"))
			throw new RuleNotApplicatbleException("valueMap parameter is required");

		try {
			if (!((Map<String, String>) params.get("valueMap") instanceof Map<String, String>)) {

				throw new RuleNotApplicatbleException("valueMap  parameter is invalid");
			}
		} catch (Exception e) {
			throw new RuleNotApplicatbleException("valueMap  parameter is invalid");
		}
		return true;
	}

	@Override
	public Dataset<Row> apply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {
		if (canApply(df, params)) {

			String aliasColumn = (String) params.get("aliasColumn");
			String originalColumn = (String) params.get("originalColumn");
			@SuppressWarnings("unchecked")
			Map<String, String> valueMap = (Map<String, String>) params.get("valueMap");

			StringBuffer buffer = new StringBuffer();
			buffer.append(" case ");
			for(String key : valueMap.keySet()) {
				buffer.append("when ").append(originalColumn).append(" = ").append("'").append(key).append("' then '").append(valueMap.get(key)).append("' ");
			}		
			buffer.append("else null end");
			
			return df.withColumn(aliasColumn, expr(buffer.toString()));
		}
		return df;
	}

}
