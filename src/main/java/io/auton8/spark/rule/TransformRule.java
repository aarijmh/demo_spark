package io.auton8.spark.rule;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.substring;

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

	private TransformInterface<Column> substringLambda = (Column column, Map<String, Object> params) -> {
		int start = ((Double)params.get("start")).intValue();
		int length = ((Double)params.get("length")).intValue();
		return substring(column, start, length);
	};

	@Override
	public boolean canApply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {
		if (params == null)
			throw new RuleNotApplicatbleException("Parameters cannot be null");
		if (!params.containsKey("originalColumn"))
			throw new RuleNotApplicatbleException("Original Column parameter is required");
		if (!params.containsKey("aliasColumn"))
			throw new RuleNotApplicatbleException("Alias Column parameter is required");
		if (!params.containsKey("type"))
			throw new RuleNotApplicatbleException("type is required as a rule parameter");
		if (!params.containsKey("start"))
			throw new RuleNotApplicatbleException("start is required as a rule parameter");
		if (!params.containsKey("length"))
			throw new RuleNotApplicatbleException("length is required as a rule parameter");

		return true;
	}

	@Override
	public Dataset<Row> apply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {
		if (canApply(df, params)) {

			String aliasColumn = (String) params.get("aliasColumn");
			String originalColumn = (String) params.get("originalColumn");
			String type = (String) params.get("type");

			TransformInterface<Column> ti = null;

			if (type.equals("substring"))
				ti = substringLambda;
			else
				throw new RuleNotApplicatbleException("Type is undefined for transform rule " + originalColumn);

			return df.withColumn(aliasColumn, ti.transform(col(originalColumn), params));
		}
		return df;
	}

}
