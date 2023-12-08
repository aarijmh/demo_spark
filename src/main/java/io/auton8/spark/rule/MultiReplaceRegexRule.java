package io.auton8.spark.rule;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.when;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import io.auton8.spark.exceptions.RuleNotApplicatbleException;
import static io.auton8.spark.utility.UtilityFunctions.normalizeColumnNameForDF;

public class MultiReplaceRegexRule implements IRule {
	
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
		if (!params.containsKey("regular_expressions"))
			throw new RuleNotApplicatbleException("regular_expressions value parameter is required");
		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Dataset<Row> apply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {
		if (canApply(df, params)) {

			String aliasColumn = (String) params.get("aliasColumn");
			String originalColumn = (String) params.get("originalColumn");
			
			
			List<Map<String, String>>  regularRexpressions = (List<Map<String, String>>) params.get("regular_expressions");
			
			for(Map<String, String> regularExpression : regularRexpressions) {
				String findReg = (String)regularExpression.get("findReg");
				String replaceValue = (String) regularExpression.get("replaceValue");
				
				String defaultValue = regularExpression.containsKey("defaultValue") ? (String) regularExpression.get("defaultValue") : replaceValue;
				df = df.withColumn(aliasColumn, when(col(originalColumn).isNull(), lit(defaultValue)).otherwise(regexp_replace(col(originalColumn),findReg, replaceValue)));
				originalColumn = normalizeColumnNameForDF( aliasColumn);
			}
		}
		return df;
	}

}
