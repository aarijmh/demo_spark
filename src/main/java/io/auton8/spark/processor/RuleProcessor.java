package io.auton8.spark.processor;

import static io.auton8.spark.utility.UtilityFunctions.normalizeColumnNameForDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.not;
import static org.apache.spark.sql.functions.when;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

import io.auton8.spark.exceptions.RuleNotApplicatbleException;
import io.auton8.spark.fileInput.FileColumn;
import io.auton8.spark.fileInput.FileRule;
import io.auton8.spark.fileInput.InputFile;
import io.auton8.spark.rule.loader.RuleLoader;

public class RuleProcessor {

	public static SparkSession getSparkSession() {
		return SparkSession.builder().appName("Customer Aggregation pipeline").master("local").getOrCreate();
	}

	public static Dataset<Row> createDFFromJSON(InputFile inputFile)
			throws JsonSyntaxException, JsonIOException, FileNotFoundException {

		Dataset<Row> df = getSparkSession().read().options(inputFile.getFileOptions()).csv(inputFile.getFileName());

		for (FileColumn fileColumn : inputFile.getColumns()) {
			if (fileColumn.getAliasName() == null && fileColumn.getRules() == null) {
				continue;
			}

			String originalName = normalizeColumnNameForDF(fileColumn.getColumnName());
			String aliasName = fileColumn.getAliasName();

			if (fileColumn.getRules() != null) {
				for (FileRule fileRule : fileColumn.getRules()) {
					fileRule.getParams().put("sparkSession", getSparkSession());
					fileRule.getParams().put("originalColumn", originalName);
					if (fileColumn.getAliasName() != null)
						fileRule.getParams().put("aliasColumn", aliasName);
					try {
						df = RuleLoader.getRuleMap().get(fileRule.getRuleName()).process(df, fileRule.getParams());
					} catch (RuleNotApplicatbleException e) {
						System.out.println(
								"Column name : " + fileColumn.getColumnName() + " -- " + fileRule.getRuleName());
						e.printStackTrace();
					}
				}
			}

		}
		return df;
	}

	private static String pattern = "yyyyMMddHHmm";
	private static SimpleDateFormat format = new SimpleDateFormat(pattern);

	public static void writeDF(Dataset<Row> df, Column[] cols, String folderPath, String delimeter, String outfileName,
			Boolean header) {
		df.select(cols).coalesce(1).write().mode(SaveMode.Overwrite).option("header", header)
				.option("delimiter", delimeter).csv(folderPath);

		File[] files = (new File(folderPath)).listFiles(x -> {
			if (x.isDirectory())
				return false;
			if (x.getName().startsWith("part") && x.getName().endsWith(".csv"))
				return true;
			return false;
		});

		if (files.length > 0) {
			files[0].renameTo(new File(
					folderPath + File.separator + outfileName.replace("%DATETIME%", format.format(new Date()))));
		}
	}

	public static Column[] fetchColumnsToWrite(Dataset<Row> df, InputFile inputFile) {
		Column[] cols = inputFile.getColumns().stream().map(fileColumn -> {
			if (fileColumn.getAliasName() == null)
				return fileColumn.getColumnName();
			return fileColumn.getAliasName();
		}).map(x -> {
			return col(normalizeColumnNameForDF(x));
		}).collect(Collectors.toList()).toArray(new Column[0]);

		return cols;
	}

	public static InputFile readConfiguration(String configurationFilePath)
			throws JsonSyntaxException, JsonIOException, FileNotFoundException {
		Gson gson = new Gson();

		return gson.fromJson(new FileReader(new File(configurationFilePath)), InputFile.class);
	}

	public static Dataset<Row> createCompareDataset(Dataset<Row> df, InputFile inputFile) {

		List<String> colNames = new ArrayList<String>();
		for (FileColumn fileColumn : inputFile.getColumns()) {
			if (fileColumn.getRules() != null) {
				for (FileRule fileRule : fileColumn.getRules()) {
					if (RuleLoader.getRuleMap().containsKey(fileRule.getRuleName())) {
						if (fileRule.getParams() != null) {
							fileRule.getParams().put("originalColumn", fileColumn.getColumnName());
							df = RuleLoader.getRuleMap().get(fileRule.getRuleName()).comparisonRuleDispatch(df,
									fileRule.getParams(), colNames);
						}
					}
				}
			} else {

				String normalizedColumnName = normalizeColumnNameForDF(fileColumn.getColumnName());
				colNames.add(fileColumn.getColumnName());

				String newName = fileColumn.getColumnName() + "_Transformed";
				colNames.add(newName);
				df = df.withColumn(newName, col(normalizedColumnName));

				String matchedName = fileColumn.getColumnName() + "=" + newName;
				colNames.add(matchedName);

				df = df.withColumn(matchedName,
						when(not(df.col(normalizedColumnName).eqNullSafe(df.col(normalizeColumnNameForDF(newName)))),
								"not matched").otherwise("matched"));

			}
		}
		Column[] cols = colNames.stream().map(x -> {
			return col(normalizeColumnNameForDF(x));
		}).collect(Collectors.toList()).toArray(new Column[0]);
		writeDF(df, cols, inputFile.getCompareLocation(), inputFile.getOutputDelimiter(),
				inputFile.getCompareTransformFileFormat(), true);
		return df;
	}

	public static void copyGeneratedFilesToFolder(InputFile inputFile, String targetFolder) throws IOException {
		Path sourceDirectory = Paths.get(inputFile.getCompareLocation());
		Path targetDirectory = Paths.get(targetFolder);
		
		if(!targetDirectory.toFile().exists()) {
			targetDirectory.toFile().mkdirs();
		}

		FileUtils.cleanDirectory(targetDirectory.toFile());

		FileUtils.copyDirectory(sourceDirectory.toFile(), targetDirectory.toFile());

		if(inputFile.getWithHeader())
		{
			sourceDirectory = Paths.get(inputFile.getSaveLocationHeader());
			FileUtils.copyDirectory(sourceDirectory.toFile(), targetDirectory.toFile());
		}
		
		if(inputFile.getWithoutHeader()) {
			sourceDirectory = Paths.get(inputFile.getSaveLocation());
			FileUtils.copyDirectory(sourceDirectory.toFile(), targetDirectory.toFile());
		}
	}

	public static void main(String[] args) throws JsonSyntaxException, JsonIOException, IOException {

		long start = System.currentTimeMillis();
		InputFile inputFile = readConfiguration("/home/hadoop/Downloads/03 Limits/JSON/limits.json");
		long fileRead = System.currentTimeMillis();

		Dataset<Row> df = createDFFromJSON(inputFile);
		long dfCreated = System.currentTimeMillis();
		Column[] cols = fetchColumnsToWrite(df, inputFile);

		if (inputFile.getWithoutHeader())
			writeDF(df, cols, inputFile.getSaveLocation(), inputFile.getOutputDelimiter(),
					inputFile.getTransformFileFormat(), false);
		if (inputFile.getWithHeader())
			writeDF(df, cols, inputFile.getSaveLocationHeader(), inputFile.getOutputDelimiter(),
					inputFile.getTransformFileHeaderFormat(), inputFile.getWithHeader());

		long fileWritten = System.currentTimeMillis();

		System.out.println(String.format("Time taken to read %d, to create DF %d and to write %d", (fileRead - start),
				(dfCreated - fileRead), (fileWritten - dfCreated)));

		System.out.println("Comparing");
		df = createCompareDataset(df, inputFile);
		long transformTime = System.currentTimeMillis();

		System.out.println(String.format("Transformation took %d", (transformTime - fileWritten)));
		
		copyGeneratedFilesToFolder(inputFile,"/home/hadoop/Downloads/GeneratedFiles");
	}

}
