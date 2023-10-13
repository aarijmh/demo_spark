package io.auton8.spark.rule;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

import java.util.Map;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.auto.service.AutoService;

import io.auton8.spark.annotations.RuleAnnotation;
import io.auton8.spark.exceptions.RuleNotApplicatbleException;
import io.auton8.spark.interfaces.ApplyInterface;

@AutoService(IRule.class)
@RuleAnnotation(ruleName = "replaceRule")
public class ReplaceRule implements IRule {

	@SuppressWarnings("unchecked")
	@Override
	public boolean canApply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {

		if (params == null)
			throw new RuleNotApplicatbleException("Parameters cannot be null");
		if (!params.containsKey("originalColumn"))
			throw new RuleNotApplicatbleException("Original Column parameter is required");
		if (!params.containsKey("aliasColumn"))
			throw new RuleNotApplicatbleException("Alias Column parameter is required");
		if (!params.containsKey("thenValue"))
			throw new RuleNotApplicatbleException("Then value parameter is required");
		if (!params.containsKey("tai"))
			throw new RuleNotApplicatbleException("TAI  parameter is required");
		try {
			if (!((ApplyInterface<Column>) params.get("tai") instanceof ApplyInterface<Column>)) {

				throw new RuleNotApplicatbleException("TAI  parameter is invalid");
			}
		} catch (Exception e) {
			throw new RuleNotApplicatbleException("TAI  parameter is invalid");
		}
		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Dataset<Row> apply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {
		if (canApply(df, params)) {

			String aliasColumn = (String) params.get("aliasColumn");
			String originalColumn = (String) params.get("originalColumn");
			String thenValue = (String) params.get("thenValue");

			ApplyInterface<Column> tai = (ApplyInterface<Column>) params.get("tai");

			return df.withColumn(aliasColumn,
					when(tai.apply(df.col(originalColumn)), thenValue).otherwise(col(originalColumn)));
		}
		return null;
	}

}
