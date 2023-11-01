package io.auton8.spark.rule;

import static io.auton8.spark.utility.UtilityFunctions.normalizeColumnNameForDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.not;
import static org.apache.spark.sql.functions.when;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import io.auton8.spark.exceptions.RuleNotApplicatbleException;

public interface IRule {

	boolean canApply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException;

	Dataset<Row> apply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException;

	default String resolveSameColumnName(String originalColumn, String aliasColumn) {
		if (aliasColumn != null && originalColumn.replaceAll("`", "").equals(aliasColumn))
			return aliasColumn + "_Transformed";
		return aliasColumn;
	}

	default Dataset<Row> process(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {
//		if(canApply(df, params)) {
		if (params.containsKey("aliasColumn")) {
			String originalColumn = (String) params.get("originalColumn");
			String aliasColumn = (String) params.get("aliasColumn");
			params.put("aliasColumn", resolveSameColumnName(originalColumn, aliasColumn));
		}
		return apply(df, params);
//		}
//		return df;
	}
	
	default Dataset<Row> comparisonRuleDispatch(Dataset<Row> df, Map<String, Object> params, List<String> cols){
		df = manageOriginalColumn(df, params, cols);
		return comparisonRule(df, params, cols);
	}
	
	default Dataset<Row> manageOriginalColumn(Dataset<Row> df, Map<String, Object> params, List<String> cols){
		String originalColumn = params.containsKey("originalColumn") ? (String) params.get("originalColumn") : null;
		String normalizedOriginalColumn = normalizeColumnNameForDF(originalColumn);
		int count = 1;
		if(cols.contains(originalColumn)) {
			String transformedColumn = originalColumn+"_"+count;
			while(cols.contains(transformedColumn)) {
				count++;
				transformedColumn = originalColumn+"_"+count;
			}
			
			df = df.withColumn(transformedColumn, col(normalizedOriginalColumn));
			params.put("originalColumn", transformedColumn); 
		}
		return df;
	}

	default Dataset<Row> comparisonRule(Dataset<Row> df, Map<String, Object> params, List<String> cols) {

		String originalColumn = params.containsKey("originalColumn") ? (String) params.get("originalColumn") : null;
		String aliasColumn = params.containsKey("aliasColumn") ? (String) params.get("aliasColumn") : null;

		aliasColumn = resolveSameColumnName(originalColumn, aliasColumn);
		String normalizedOriginalColumn = normalizeColumnNameForDF(originalColumn);
//		int count = 1;
//		if(cols.contains(originalColumn)) {
//			String transformedColumn = originalColumn+"_"+count;
//			while(cols.contains(transformedColumn)) {
//				count++;
//				transformedColumn = originalColumn+"_"+count;
//			}
//			
//			df = df.withColumn(transformedColumn, col(normalizedOriginalColumn));
//			originalColumn = transformedColumn;
//		}
//		
		cols.add(originalColumn);

		String newColumn = aliasColumn;
		if (aliasColumn == null) {
			newColumn = originalColumn + "_Transformed";
			df = df.withColumn(newColumn, col(normalizedOriginalColumn));
		}

		cols.add(newColumn);

		String compColumn = (originalColumn + "=" + newColumn).replaceAll("\\.", "_");

		cols.add(compColumn);

		try {
			df = df.withColumn(compColumn,
					when(not(df.col(normalizedOriginalColumn).eqNullSafe(df.col(normalizeColumnNameForDF(newColumn)))),
							"not matched").otherwise("matched"));
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		return df;
	}
}
