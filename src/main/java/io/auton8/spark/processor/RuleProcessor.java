package io.auton8.spark.processor;

import static org.apache.spark.sql.functions.col;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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

	public static String normalizeColumnNameForDF(String columnName) {
		if(columnName.contains("."))
			return "`"+columnName+"`";
		return columnName;
	}
	
	public static void main(String [] args) throws JsonSyntaxException, JsonIOException, FileNotFoundException {
		Gson gson = new Gson();
		InputFile inputFile = gson.fromJson(new FileReader(new File("/home/hadoop/Documents/customer.json")), InputFile.class);
		
		Dataset<Row> df = getSparkSession().read().options(inputFile.getFileOptions()).csv(inputFile.getFileName()); 
		List<String> columnNames = new ArrayList<String>();
		
		for(FileColumn fileColumn : inputFile.getColumns()) {
			if(fileColumn.getAliasName() == null && fileColumn.getRules() == null) {
				columnNames.add(fileColumn.getColumnName());
				continue;
			}
			
			String originalName = normalizeColumnNameForDF(fileColumn.getColumnName());
			String aliasName = null;
			
			if(fileColumn.getAliasName() != null) {
				aliasName = fileColumn.getAliasName();
				columnNames.add(aliasName);
			}
			else {
				columnNames.add(originalName);
			}
			
			if(fileColumn.getRules() != null) {
				for(FileRule fileRule : fileColumn.getRules()) {
					fileRule.getParams().put("sparkSession", getSparkSession());
					fileRule.getParams().put("originalColumn", originalName);
					if(fileColumn.getAliasName() != null)
						fileRule.getParams().put("aliasColumn", aliasName);
					try {
						df = RuleLoader.getRuleMap().get(fileRule.getRuleName()).apply(df, fileRule.getParams());
					} catch (RuleNotApplicatbleException e) {
						e.printStackTrace();
					}
				}
			}
			
		}
	
		df.show();
		
	    Column [] cols = columnNames.stream().map(x->col(normalizeColumnNameForDF(x))).collect(Collectors.toList()).toArray(new Column[0]); 
	    df.select(cols).write().mode(SaveMode.Overwrite).option("header", true).option("delimiter", ",")
				.csv("/home/hadoop/Downloads/TEST");
		
	}

}
