package io.auton8.spark.rule;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import io.auton8.spark.exceptions.RuleNotApplicatbleException;

public class ConcatColumnRule implements IRule {
	
	public static Dataset<Row> concatColumns(Dataset<Row> df, String aliasColumn, String column1, String column2,
			String middleString, String endString) {
		return df.withColumn(aliasColumn, concat(when(col(column1).isNull(), "").otherwise(col(column1)),
				lit(middleString), when(col(column2).isNull(), "").otherwise(col(column2)), lit(endString)));
	}

	@Override
	public boolean canApply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {
		if (params == null)
			throw new RuleNotApplicatbleException("Parameters cannot be null");
		if (!params.containsKey("column1"))
			throw new RuleNotApplicatbleException("Column1 parameter is required");
		if (!params.containsKey("column2"))
			throw new RuleNotApplicatbleException("Column2 parameter is required");
		if (!params.containsKey("aliasColumn"))
			throw new RuleNotApplicatbleException("Alias Column parameter is required");
		if (!params.containsKey("startString"))
			throw new RuleNotApplicatbleException("startString parameter is required");
		if (!params.containsKey("middleString"))
			throw new RuleNotApplicatbleException("middleString parameter is required");
		if (!params.containsKey("endString"))
			throw new RuleNotApplicatbleException("endString parameter is required");
		return true;
	}

	@Override
	public Dataset<Row> apply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {
		if (canApply(df, params)) {

			String aliasColumn = (String) params.get("aliasColumn");
			String column1 = (String) params.get("column1");
			String column2 = (String) params.get("column2");
			String startString = (String) params.get("startString");
			String middleString = (String) params.get("middleString");
			String endString = (String) params.get("endString");

			return df.withColumn(aliasColumn, concat(lit(startString),when(col(column1).isNull(), "").otherwise(col(column1)),
					lit(middleString), when(col(column2).isNull(), "").otherwise(col(column2)), lit(endString)));
		}
		return null;
	}

}
