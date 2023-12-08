package io.auton8.spark.rule;

import static io.auton8.spark.utility.UtilityFunctions.normalizeColumnNameForDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import io.auton8.spark.exceptions.RuleNotApplicatbleException;

public class ConcatenateMultiColumnRule  extends ConcatColumnRule implements IRule {

	public static Dataset<Row> concatColumns(Dataset<Row> df, String aliasColumn, String column1, String column2,
			String middleString, String endString) {
		return df.withColumn(aliasColumn, concat(when(col(column1).isNull(), "").otherwise(col(column1)),
				lit(middleString), when(col(column2).isNull(), "").otherwise(col(column2)), lit(endString)));
	}

	@Override
	public boolean canApply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {
		if (params == null)
			throw new RuleNotApplicatbleException("Parameters cannot be null");
		if (!params.containsKey("columns"))
			throw new RuleNotApplicatbleException("columns parameter is required");
		if (!params.containsKey("aliasColumn"))
			throw new RuleNotApplicatbleException("Alias Column parameter is required");
		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Dataset<Row> apply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {
		if (canApply(df, params)) {

			List<String> columns = (List<String>) params.get("columns");
			String separator = params.containsKey("separator") ? (String) params.get("separator") : "";
			
			Column [] cols = new Column[columns.size()];
			String aliasColumn = (String) params.get("aliasColumn");
			
			for(int i = 0; i < cols.length; i++) {

					cols[i] = when(df.col(columns.get(i)).isNull(), null).otherwise(df.col(columns.get(i)));
			}
			return df.withColumn(aliasColumn, concat_ws(separator,cols));
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Dataset<Row> comparisonRule(Dataset<Row> df, Map<String, Object> params, List<String> cols) {

		List<String> column1 = params.containsKey("columns") ? (List<String>) params.get("columns") : null;
		
		String aliasColumn = params.containsKey("aliasColumn") ? (String) params.get("aliasColumn") : null;
		String transformedColumnSuffix = params.containsKey("transformedColumnSuffix") ? (String) params.get("transformedColumnSuffix") : "";
		
		String suffix = "";
		int count = 0;
		for(int i = 0; i < column1.size(); i++) {

			String columnName = column1.get(i);
			if (count == 0) {
				aliasColumn = resolveSameColumnName(columnName, aliasColumn);
			}
			if( count != 0) {
				suffix = "_"+i;
				df = df.withColumn(aliasColumn+suffix, col(normalizeColumnNameForDF(aliasColumn)));
			}
			
			df = solveColumnComparison(df, columnName, aliasColumn+suffix, transformedColumnSuffix, cols);
			count++;
		}

		return df;
	}

}
