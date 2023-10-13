package io.auton8.spark.rule;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import io.auton8.spark.exceptions.RuleNotApplicatbleException;

public interface IRule {
	
	boolean canApply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException;
	
	Dataset<Row> apply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException;
}
