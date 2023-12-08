package io.auton8.spark.rule;

import static io.auton8.spark.utility.UtilityFunctions.normalizeColumnNameForDF;
import static io.auton8.spark.utility.UtilityFunctions.compareColumns;
import static org.apache.spark.sql.functions.lit;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import io.auton8.spark.exceptions.RuleNotApplicatbleException;
import io.auton8.spark.utility.Constants;

public class DefaultColumnRule implements IRule {

	public static Dataset<Row> createDefaultColumn(Dataset<Row> df, String aliasColumn, String defaultValue) {
		return df.withColumn(aliasColumn, lit(defaultValue));
	}
	
	@Override
	public boolean canApply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {
		if (params == null)
			throw new RuleNotApplicatbleException("Parameters cannot be null");
		if (!params.containsKey("aliasColumn"))
			throw new RuleNotApplicatbleException("Alias Column parameter is required");
		if (!params.containsKey("defaultValue"))
			throw new RuleNotApplicatbleException("defaultValue value parameter is required");
		return true;
	}

	@Override
	public Dataset<Row> apply(Dataset<Row> df, Map<String, Object> params) throws RuleNotApplicatbleException {
		if (canApply(df, params)) {

			String aliasColumn = (String) params.get("aliasColumn");
			String defaultValue = (String) params.get("defaultValue");
			return df.withColumn(aliasColumn, lit(defaultValue));
		}
		return null;
	}
	
	@Override
	public Dataset<Row> comparisonRule(Dataset<Row> df, Map<String, Object> params, List<String> cols){
		
		String aliasColumn = params.containsKey("aliasColumn") ? (String)params.get("aliasColumn") : null;
		
		List<String> colNames = List.of(df.columns());
		
		int emptyIndex = 0;
		
		while(colNames.contains("Empty_"+(++emptyIndex)));
		
		String newColumn = "Empty_"+emptyIndex;
		
		df = df.withColumn(newColumn, lit(""));

		cols.add(newColumn);
		cols.add(aliasColumn);
		
		String compColumn = (newColumn + "="+aliasColumn).replaceAll("\\.", "_");
		
		cols.add(compColumn);
		
		try {
		//	df = df.withColumn(compColumn, when(not(df.col(newColumn).eqNullSafe(df.col(normalizeColumnNameForDF(aliasColumn)))),"not matched").otherwise("matched"));
			df = compareColumns(df, compColumn, df.col(newColumn), df.col(normalizeColumnNameForDF(aliasColumn)), Constants.MATCHED_STRING, Constants.NOT_MATCHED_STRING);
		}
		catch(Exception e) {
			e.printStackTrace();
			cols.remove(newColumn);
			cols.remove(aliasColumn);
			cols.remove(compColumn);
			throw e;
		}
				
		return df;
	}

}
