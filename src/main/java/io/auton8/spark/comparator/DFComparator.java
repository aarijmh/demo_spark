package io.auton8.spark.comparator;

import static io.auton8.spark.utility.UtilityFunctions.normalizeColumnNameForDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.not;
import static org.apache.spark.sql.functions.trim;
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

import io.auton8.spark.exceptions.RuleNotApplicatbleException;
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

		Dataset<Row> intermediaryDF = mainDF.join(secondDF, trim(extractDF.col(normalizedColumn)).eqNullSafe(trim(secondDF.col(normalizedColumn))), "anti");

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

	public static void compareDF(Dataset<Row> sourceDF, Dataset<Row> targetDF, String keyColumn, InputFile inputFile, String outputFolder) {
		// targetDF =
		// targetDF.withColumn(normalizeColumnNameForDF(keyColumn+"_extract"),
		// col(normalizeColumnNameForDF(keyColumn)));

		/*
		 * PRE EXTRACT ROUTINE
		 */
		if (inputFile.getPreExtractCompareColumns() != null)
			for (FileColumn fileColumn : inputFile.getPreExtractCompareColumns()) {
				if (fileColumn.getAliasName() == null && fileColumn.getRules() == null) {
					continue;
				}

				String originalName = fileColumn.getColumnName() != null ? normalizeColumnNameForDF(fileColumn.getColumnName()) : null;
				String aliasName = fileColumn.getAliasName();

				if (fileColumn.getRules() != null) {
					boolean firstTime = true;
					for (FileRule fileRule : fileColumn.getRules()) {
						fileRule.getParams().put("sparkSession", getSparkSession());
						fileRule.getParams().put("originalColumn", originalName);
						fileRule.getParams().put("firstTime", firstTime);
						if (fileColumn.getAliasName() != null)
							fileRule.getParams().put("aliasColumn", aliasName);
						try {
							sourceDF = RuleLoader.getRuleMap().get(fileRule.getRuleName()).process(sourceDF, fileRule.getParams());
						} catch (RuleNotApplicatbleException e) {
							System.out.println("Column name : " + fileColumn.getColumnName() + " -- " + fileRule.getRuleName());
							e.printStackTrace();
						}
						if (fileColumn.getAliasName() != null) {
							originalName = aliasName;
						}
						firstTime = false;
					}
				}

			}
		//sourceDF.where(col("SECURITY_ID").contains("R*19526")).write().mode(SaveMode.Overwrite).option("header", true).option("delimiter", ",").option("quote", "").option("escape", "\"").csv("d:\\test2");;
		//sourceDF.coalesce(1).write().mode(SaveMode.Overwrite).option("header", true).option("delimiter", ",").option("quote", "").option("escape", "\"").csv("d:\\test2");
		/*
		 * END PRE EXTRACT ROUTINE
		 */
		
		/*
		 * PRE EXTRACT TARGET ROUTINE
		 */
		if (inputFile.getPreExtractCompareColumnsTarget() != null)
			for (FileColumn fileColumn : inputFile.getPreExtractCompareColumnsTarget()) {
				if (fileColumn.getAliasName() == null && fileColumn.getRules() == null) {
					continue;
				}

				String originalName = fileColumn.getColumnName() != null ? normalizeColumnNameForDF(fileColumn.getColumnName()) : null;
				String aliasName = fileColumn.getAliasName();

				if (fileColumn.getRules() != null) {
					boolean firstTime = true;
					for (FileRule fileRule : fileColumn.getRules()) {
						fileRule.getParams().put("sparkSession", getSparkSession());
						fileRule.getParams().put("originalColumn", originalName);
						fileRule.getParams().put("firstTime", firstTime);
						if (fileColumn.getAliasName() != null)
							fileRule.getParams().put("aliasColumn", aliasName);
						try {
							targetDF = RuleLoader.getRuleMap().get(fileRule.getRuleName()).process(targetDF, fileRule.getParams());
						} catch (RuleNotApplicatbleException e) {
							System.out.println("Column name : " + fileColumn.getColumnName() + " -- " + fileRule.getRuleName());
							e.printStackTrace();
						}
						if (fileColumn.getAliasName() != null) {
							originalName = aliasName;
						}
						firstTime = false;
					}
				}

			}
		//sourceDF.where(col("SECURITY_ID").contains("R*19526")).write().mode(SaveMode.Overwrite).option("header", true).option("delimiter", ",").option("quote", "").option("escape", "\"").csv("d:\\test2");;
		//sourceDF.coalesce(1).write().mode(SaveMode.Overwrite).option("header", true).option("delimiter", ",").option("quote", "").option("escape", "\"").csv("d:\\test2");
		/*
		 * END PRE EXTRACT TARGET ROUTINE
		 */
		
		
		//targetDF.show(10);
		
		String normalizedKeyColumn = normalizeColumnNameForDF(keyColumn);
		String normalizedExtractKeyColumn = normalizeColumnNameForDF(keyColumn + "_Extract");
		System.out.println(targetDF.count());
		for (String columnName : targetDF.columns()) {
			targetDF = targetDF.withColumnRenamed(columnName, columnName + "_Extract");
		}
		Dataset<Row> df = targetDF.join(sourceDF, trim(targetDF.col(normalizedExtractKeyColumn)).eqNullSafe(trim(sourceDF.col(normalizedKeyColumn))), "full");

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
								fileRule.getParams().put("aliasColumn",
										existingColumnNames.contains(fileColumn.getAliasName() + "_Extract") ? fileColumn.getAliasName() + "_Extract" : inputFile.getAliases().get(fileColumn.getAliasName()) + "_Extract");
								fileRule.getParams().put("transformedColumnSuffix", transformedColumnSuffix);

								df = RuleLoader.getRuleMap().get(fileRule.getRuleName()).comparisonRuleDispatch(df, fileRule.getParams(), colNames);
							}
						}
						break;
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
		RuleProcessor.writeDF(df, cols, outputFolder, "|", inputFile.getCompareExtractFileFormat(), true);

		// df.show();
	}

	public static void main(String[] args) throws JsonSyntaxException, JsonIOException, FileNotFoundException {
		
		String baseFolder = "C:\\Users\\Lenovo\\OneDrive - auton8.io\\auton8\\";

		/*
		 * InputFile inputFile = RuleProcessor.readConfiguration(
		 * baseFolder+"04 Collateral\\02 MOTOR VEHICLE\\JSON\\motor_vehicle.json");
		 * 
		 * Dataset<Row> sourceDF =
		 * getSparkSession().read().options(inputFile.getFileOptions()).csv(
		 * baseFolder+"04 Collateral\\02 MOTOR VEHICLE\\source\\M_COLLAT.20232911.txt");
		 * 
		 * Dataset<Row> extractDF =
		 * getSparkSession().read().options(inputFile.getFileOptions()).csv(
		 * baseFolder+"04 Collateral\\02 MOTOR VEHICLE\\extract\\COLLATERAL.VEHICLE.AUTON8.202312110230.txt"
		 * );
		 * 
		 * compareDF(sourceDF, extractDF, "NOTES", inputFile,
		 * baseFolder+"04 Collateral\\02 MOTOR VEHICLE\\comparison\\extract");
		 */
		
		/*
		 * InputFile inputFile = RuleProcessor.readConfiguration(
		 * baseFolder+"04 Collateral\\03 BOAT\\JSON\\boat.json");
		 * 
		 * Dataset<Row> sourceDF =
		 * getSparkSession().read().options(inputFile.getFileOptions()).csv(
		 * baseFolder+"04 Collateral\\03 BOAT\\source\\B_COLLAT.20232911.txt");
		 * 
		 * Dataset<Row> extractDF =
		 * getSparkSession().read().options(inputFile.getFileOptions()).csv(
		 * baseFolder+"04 Collateral\\03 BOAT\\extract\\COLLATERAL.BOAT.AUTON8.202312110230-EXTRACT.txt"
		 * );
		 * 
		 * compareDF(sourceDF, extractDF, "NOTES", inputFile,
		 * baseFolder+"04 Collateral\\03 BOAT\\comparison\\extract");
		 */	
		/*
		 * InputFile inputFile = RuleProcessor.readConfiguration(
		 * baseFolder+"04 Collateral\\04 Real State\\JSON\\real_state.json"); //
		 * Dataset<Row> sourceDF =
		 * getSparkSession().read().options(inputFile.getFileOptions()).csv(
		 * baseFolder+"04 Collateral\\04 Real State\\source\\R_COLLAT.20230412.txt"); //
		 * Dataset<Row> extractDF =
		 * getSparkSession().read().options(inputFile.getFileOptions()).csv(
		 * baseFolder+"04 Collateral\\04 Real State\\extract\\COLLATERAL.REAL.ESTATE.AUTON8.202312110230.txt"
		 * ); // compareDF(sourceDF, extractDF, "NOTES", inputFile,
		 * baseFolder+"04 Collateral\\04 Real State\\comparison\\extract");
		 */
		
		InputFile inputFile = RuleProcessor.readConfiguration(baseFolder+"04 Collateral\\01 Asset Reg Property\\JSON\\asst_reg_property.json");

		Dataset<Row> sourceDF = getSparkSession().read().options(inputFile.getFileOptions()).csv(baseFolder+"\\04 Collateral\\01 Asset Reg Property\\source\\R_COLLAT.20230412.txt");

		Dataset<Row> extractDF = getSparkSession().read().options(inputFile.getFileOptions()).csv(baseFolder+"\\04 Collateral\\01 Asset Reg Property\\extract\\ASSET.REG.PROPERTY.AUTON8.202312060646.txt");

		compareDF(sourceDF, extractDF, "SECURITY_ID", inputFile, baseFolder+"\\04 Collateral\\01 Asset Reg Property\\comparison\\extract");


	}

}
