package io.auton8.spark.rule;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.to_timestamp;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import io.auton8.spark.exceptions.RuleNotApplicatbleException;

public class ModifyDateRule implements IRule {
	
	@SuppressWarnings("unchecked")
	@Override
	public boolean canApply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {
		if (params == null)
			throw new RuleNotApplicatbleException("Parameters cannot be null");
		if (!params.containsKey("originalColumn"))
			throw new RuleNotApplicatbleException("Original Column parameter is required");
		if (!params.containsKey("aliasColumn"))
			throw new RuleNotApplicatbleException("Alias Column parameter is required");
		if (!params.containsKey("formats"))
			throw new RuleNotApplicatbleException("formats value parameter is required");
		try {
			if (!((List<Map<String,String>>) params.get("formats") instanceof List<Map<String,String>>)) {

				throw new RuleNotApplicatbleException("formats  parameter is invalid");
			}
		} catch (Exception e) {
			throw new RuleNotApplicatbleException("formats  parameter is invalid");
		}
		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Dataset<Row> apply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {
		if (canApply(df, params)) {

			String aliasColumn = (String) params.get("aliasColumn");
			String originalColumn = (String) params.get("originalColumn");
			List<Map<String,String>> formats = (List<Map<String,String>>) params.get("formats");
			
			

			Column[] cols = new Column[formats.size()];

			int index = 0;
			for (Map<String,String> pair : formats) {			
				cols[index++] = date_format(to_timestamp(col(originalColumn), pair.get("source")), pair.get("target"));
			}

			return df.withColumn(aliasColumn, coalesce(cols));
		}
		return null;
	}

}
