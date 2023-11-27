package io.auton8.spark.comparator;

import static io.auton8.spark.utility.UtilityFunctions.normalizeColumnNameForDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.not;
import static org.apache.spark.sql.functions.when;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

import io.auton8.spark.fileInput.FileColumn;
import io.auton8.spark.fileInput.FileRule;
import io.auton8.spark.fileInput.InputFile;
import io.auton8.spark.processor.RuleProcessor;
import io.auton8.spark.rule.loader.RuleLoader;

public class DFComparator {
	public static SparkSession getSparkSession() {
		return SparkSession.builder().appName("Customer Aggregation pipeline").master("local").getOrCreate();
	}

	public static void normalizeDatasets(Dataset<Row> sourceDF, Dataset<Row> extractDF, String targetColumn) {
		long sourceCount = sourceDF.count();
		long extractCount = extractDF.count();

		String normalizedColumn = normalizeColumnNameForDF(targetColumn);

		Dataset<Row> mainDF = null;
		Dataset<Row> secondDF = null;

		if (sourceCount > extractCount) {
			mainDF = sourceDF;
			secondDF = extractDF;
		} else {
			mainDF = extractDF;
			secondDF = sourceDF;
		}

		Dataset<Row> intermediaryDF = mainDF.join(secondDF, extractDF.col(normalizedColumn).eqNullSafe(secondDF.col(normalizedColumn)), "anti");

		Row[] row = (Row[]) intermediaryDF.select(normalizedColumn).collect();

		int targetIndex = -1;
		for (int i = 0; i < secondDF.columns().length; i++) {
			if (secondDF.columns()[i].equals(targetColumn)) {
				targetIndex = i;
				break;
			}
		}

		List<Row> rows = new ArrayList<Row>();
		int schemaLength = secondDF.schema().fields().length;
		for (int i = 0; i < row.length; i++) {
			Object[] o = new Object[schemaLength];
			o[targetIndex] = row[i].get(0).toString();
			rows.add(RowFactory.create(o));
		}

		Dataset<Row> schema = getSparkSession().createDataFrame(rows, secondDF.schema());
		secondDF = secondDF.union(schema);
	}

	public static void compareDF(Dataset<Row> sourceDF, Dataset<Row> targetDF, String keyColumn, InputFile inputFile) {
		// targetDF =
		// targetDF.withColumn(normalizeColumnNameForDF(keyColumn+"_extract"),
		// col(normalizeColumnNameForDF(keyColumn)));
		String normalizedKeyColumn = normalizeColumnNameForDF(keyColumn);
		String normalizedExtractKeyColumn = normalizeColumnNameForDF(keyColumn + "_Extract");
		System.out.println(targetDF.count());
		for (String columnName : targetDF.columns()) {
			targetDF = targetDF.withColumnRenamed(columnName, columnName + "_Extract");
		}
		Dataset<Row> df = targetDF.join(sourceDF, targetDF.col(normalizedExtractKeyColumn).eqNullSafe(sourceDF.col(normalizedKeyColumn)), "leftouter");

		List<String> existingColumnNames = List.of(df.columns());

		String transformedColumnSuffix = "";
		List<String> colNames = new ArrayList<String>();
		for (FileColumn fileColumn : inputFile.getColumns()) {
			try {
				if (fileColumn.getRules() != null) {
					for (FileRule fileRule : fileColumn.getRules()) {
						if (RuleLoader.getRuleMap().containsKey(fileRule.getRuleName())) {
							if (fileRule.getParams() != null) {
								fileRule.getParams().put("originalColumn", fileColumn.getColumnName());
								fileRule.getParams().put("aliasColumn", existingColumnNames.contains(fileColumn.getAliasName()+"_Extract") ? fileColumn.getAliasName() + "_Extract" : inputFile.getAliases().get(fileColumn.getAliasName()) + "_Extract");
								fileRule.getParams().put("transformedColumnSuffix", transformedColumnSuffix);

								df = RuleLoader.getRuleMap().get(fileRule.getRuleName()).comparisonRuleDispatch(df, fileRule.getParams(), colNames);
							}
						}
					}
				} else {

					String normalizedColumnName = normalizeColumnNameForDF(fileColumn.getColumnName());
					colNames.add(fileColumn.getColumnName());

					String newName = fileColumn.getColumnName() + "_Extract";
					colNames.add(newName);
					df = df.withColumn(newName, col(normalizedColumnName));

					String matchedName = fileColumn.getColumnName() + "=" + newName;
					colNames.add(matchedName);

					df = df.withColumn(matchedName, when(not(df.col(normalizedColumnName).eqNullSafe(df.col(normalizeColumnNameForDF(newName)))), "not matched").otherwise("matched"));

				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		Column[] cols = colNames.stream().map(x -> {
			return col(normalizeColumnNameForDF(x));
		}).collect(Collectors.toList()).toArray(new Column[0]);
		RuleProcessor.writeDF(df, cols, "d:\\TEST COMPARE", ",", inputFile.getCompareTransformFileFormat(), true);

		//df.show();
	}

	public static void main(String[] args) throws JsonSyntaxException, JsonIOException, FileNotFoundException {

		InputFile inputFile = RuleProcessor.readConfiguration("D:\\auton8\\01 Customer\\JSON\\customer.json");

		Dataset<Row> sourceDF = getSparkSession().read().option("header", true).option("delimiter", "|").csv("D:\\auton8\\01 Customer\\source\\CUSTOM.20230409.txt");

		Dataset<Row> extractDF = getSparkSession().read().option("header", true).option("delimiter", "|").csv("D:\\auton8\\01 Customer\\extract\\CUSTOMER.01.202326090130-EXTRACT.txt");

		compareDF(sourceDF, extractDF, "CUSTOMER.CODE", inputFile);
	}

}
