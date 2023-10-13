package io.auton8.spark.rule;

import static org.apache.spark.sql.functions.col;

import java.util.Map;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import io.auton8.spark.exceptions.RuleNotApplicatbleException;
import io.auton8.spark.interfaces.TransformInterface;

public class TransformRule implements IRule {
	
//	public static Dataset<Row> transformValue(Dataset<Row> df, String originalColumn, String aliasColumn,
//			TransformInterface<Column> ti, Map<String, Object> params) {
//		return df.withColumn(aliasColumn, ti.transform(col(originalColumn), params));
//	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean canApply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {
		if (params == null)
			throw new RuleNotApplicatbleException("Parameters cannot be null");
		if (!params.containsKey("originalColumn"))
			throw new RuleNotApplicatbleException("Original Column parameter is required");
		if (!params.containsKey("aliasColumn"))
			throw new RuleNotApplicatbleException("Alias Column parameter is required");
		if (!params.containsKey("params"))
			throw new RuleNotApplicatbleException("params value parameter is required");
		if (!params.containsKey("ti"))
			throw new RuleNotApplicatbleException("TI  parameter is required");
		try {
			if (!((TransformInterface<Column>) params.get("ti") instanceof TransformInterface<Column>)) {

				throw new RuleNotApplicatbleException("TI  parameter is invalid");
			}
		} catch (Exception e) {
			throw new RuleNotApplicatbleException("TI  parameter is invalid");
		}
		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Dataset<Row> apply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {
		if (canApply(df, params)) {

			String aliasColumn = (String) params.get("aliasColumn");
			String originalColumn = (String) params.get("originalColumn");
			Map<String, Object> thenValue = (Map<String, Object>) params.get("params");

			TransformInterface<Column> ti = (TransformInterface<Column>) params.get("ti");

			return df.withColumn(aliasColumn, ti.transform(col(originalColumn), thenValue));
		}
		return df;
	}

}
