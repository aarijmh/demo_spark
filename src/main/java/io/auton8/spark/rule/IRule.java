package io.auton8.spark.rule;

import static io.auton8.spark.utility.UtilityFunctions.compareColumns;
import static io.auton8.spark.utility.UtilityFunctions.normalizeColumnNameForDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import io.auton8.spark.exceptions.RuleNotApplicatbleException;
import io.auton8.spark.utility.Constants;

public interface IRule {

	boolean canApply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException;

	Dataset<Row> apply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException;

	default String resolveSameColumnName(String originalColumn, String aliasColumn) {
		if (aliasColumn != null && originalColumn != null && originalColumn.replaceAll("`", "").equals(aliasColumn))
			return aliasColumn + "_Transformed";
		return aliasColumn;
	}

	default Dataset<Row> process(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {
//		if(canApply(df, params)) {
		if (params.containsKey("aliasColumn")) {
			String originalColumn = params.containsKey("originalColumn") ? (String) params.get("originalColumn") : null;
			String aliasColumn = (String) params.get("aliasColumn");
			if(params.containsKey("firstTime") && (boolean)params.get("firstTime"))
				params.put("aliasColumn", resolveSameColumnName(originalColumn, aliasColumn));
		}
		return apply(df, params);
//		}
//		return df;
	}

	default Dataset<Row> comparisonRuleDispatch(Dataset<Row> df, Map<String, Object> params, List<String> cols) {
		df = manageOriginalColumn(df, params, cols);
		return comparisonRule(df, params, cols);
	}

	default Dataset<Row> replaceDuplicateColumnName(Dataset<Row> df, Map<String, Object> params, List<String> cols, String originalColumn, String columnKey) {
		String normalizedOriginalColumn = normalizeColumnNameForDF(originalColumn);
		int count = 1;
		if (cols.contains(originalColumn)) {
			String transformedColumn = originalColumn + "_" + count;
			while (cols.contains(transformedColumn)) {
				count++;
				transformedColumn = originalColumn + "_" + count;
			}

			if (columnKey != null)
				params.put(columnKey, transformedColumn);
			df = df.withColumn(transformedColumn, col(normalizedOriginalColumn));
		}
		return df;
	}

	default Dataset<Row> manageOriginalColumn(Dataset<Row> df, Map<String, Object> params, List<String> cols) {
		String originalColumn = params.containsKey("originalColumn") ? (String) params.get("originalColumn") : null;
//		String normalizedOriginalColumn = normalizeColumnNameForDF(originalColumn);
//		int count = 1;
//		if(cols.contains(originalColumn)) {
//			String transformedColumn = originalColumn+"_"+count;
//			while(cols.contains(transformedColumn)) {
//				count++;
//				transformedColumn = originalColumn+"_"+count;
//			}
//			
//			df = df.withColumn(transformedColumn, col(normalizedOriginalColumn));
//			params.put("originalColumn", transformedColumn); 
//		}
		// return df;
		if (originalColumn != null)
			return replaceDuplicateColumnName(df, params, cols, originalColumn, "originalColumn");
		return df;
	}

	default Dataset<Row> comparisonRule(Dataset<Row> df, Map<String, Object> params, List<String> cols) {

		String originalColumn = params.containsKey("originalColumn") ? (String) params.get("originalColumn") : null;
		String aliasColumn = params.containsKey("aliasColumn") ? (String) params.get("aliasColumn") : null;
		String transformedColumnSuffix = params.containsKey("transformedColumnSuffix") ? (String) params.get("transformedColumnSuffix") : "";
		;

		if (originalColumn == null) {
			String na = "Empty";
			Stream<String> stream = Arrays.stream(df.columns());
			Map<String, String> nameMap = stream.collect(Collectors.toMap(x -> x, x -> x));
			int count = 0;
			while (nameMap.containsKey(na + "_" + (++count)))
				;
			df = df.withColumn(na + "_" + count, lit(""));
			originalColumn = na + "_" + count;
		}
		aliasColumn = resolveSameColumnName(originalColumn, aliasColumn);
		String normalizedOriginalColumn = normalizeColumnNameForDF(originalColumn);
		cols.add(originalColumn);

		String newColumn = aliasColumn;
		if (aliasColumn == null) {
			newColumn = originalColumn + transformedColumnSuffix;
			df = df.withColumn(newColumn, col(normalizedOriginalColumn));
		}

		// newColumn = normalizeColumnNameForDF(newColumn);
		cols.add(newColumn);

		String compColumn = (originalColumn + "=" + newColumn).replaceAll("\\.", "_");

		cols.add(compColumn);

		try {
			String newColumn1 = normalizeColumnNameForDF(newColumn);
			df = compareColumns(df, compColumn, df.col(normalizedOriginalColumn), df.col(newColumn1), Constants.MATCHED_STRING, Constants.NOT_MATCHED_STRING);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		return df;
	}

}
