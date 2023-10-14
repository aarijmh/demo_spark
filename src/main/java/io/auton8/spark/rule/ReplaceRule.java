package io.auton8.spark.rule;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lower;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.not;

import java.util.List;
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
		if (!params.containsKey("condition"))
			throw new RuleNotApplicatbleException("condition  parameter is required");
		return true;
	}

	@SuppressWarnings("unused")
	@Override
	public Dataset<Row> apply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {
		if (canApply(df, params)) {

			String aliasColumn = (String) params.get("aliasColumn");
			String originalColumn = (String) params.get("originalColumn");
			String thenValue = (String) params.get("thenValue");
			String condition = (String) params.get("condition");

			ApplyInterface<Column> tai = (Column col) -> {
				if(condition.equals("isNull"))
					return col.isNull();
				else if(condition.equals("isIn")) {
					@SuppressWarnings("unchecked")
					List<String> values = (List<String>) params.get("values");
					Object [] strs = new Object[values.size()];
					for(int i = 0; i < values.size(); i++)
						strs[i] = values.get(i).toLowerCase();
					return lower(col).isin(strs);
				}else if(condition.equals("isNotIn")) {
					@SuppressWarnings("unchecked")
					List<String> values = (List<String>) params.get("values");
					Object [] strs = new Object[values.size()];
					for(int i = 0; i < values.size(); i++)
						strs[i] = values.get(i).toLowerCase();
					return not(lower(col).isin(strs));
				}
				return col;
			};

			return df.withColumn(aliasColumn,
					when(tai.apply(df.col(originalColumn)), thenValue).otherwise(col(originalColumn)));
		}
		return df;
	}

}
