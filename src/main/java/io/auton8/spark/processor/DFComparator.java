package io.auton8.spark.processor;

import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import io.auton8.spark.rule.loader.RuleLoader;

import static org.apache.spark.sql.functions.*;

public class DFComparator {
	
	public static SparkSession getSparkSession() {
		return SparkSession.builder().appName("Customer Aggregation pipeline").master("local").getOrCreate();
	}

	public static String normalizeNames(String name) {
		if(name.contains(".")) {
			return "`" + name+"`";
		}
		return name;
	}

	public static void main(String[] args) {
		RuleLoader.getRuleMap();
		Dataset<Row> javaDF = getSparkSession().read().option("header", true).option("delimiter", ",")
				.csv("/home/hadoop/Downloads/CUSTOMER/part-00000-4d7317d5-6bc5-407b-b9fe-56fb54e3e3f3-c000.csv");

		
		Dataset<Row> rDF = getSparkSession().read().option("header", true).option("delimiter", "|")
				.csv("/home/hadoop/Downloads/CUSTOMER.AUTON8.202321091251_header.txt");

		

		String [] javaColumns = javaDF.columns();
		String [] rColumns = rDF.columns();
		
		for(int i = 0; i < javaColumns.length; i++) {
			String newJavaColumn = javaColumns[i];
			String rColumn = new String(rColumns[i]);
			
//			if(newJavaColumn.contains(".")) {
//				javaDF = javaDF.withColumnRenamed("`"+newJavaColumn+"`", newJavaColumn.replace('.', '_'));
//			}
			
//			if(rColumn.contains(".")) {
//				rColumn = rColumn.replace('.', '_').concat("rightDF");
//				rDF = rDF.withColumnRenamed(rColumns[i], rColumn);
//				
//			}else
//			{
//				
//			}
			rDF = rDF.withColumnRenamed(rColumns[i], rColumn.concat("rightDF"));
		}
		
		javaDF = javaDF.withColumn("unique_row_id", monotonically_increasing_id());
		rDF = rDF.withColumn("unique_row_id", monotonically_increasing_id());
	
		Dataset<Row> merged = javaDF.join(rDF,"unique_row_id","outer").drop("unique_row_id");
		

		//System.out.println(merged.count());
		//merged.select("`UPLOAD.COMPANY`").show();
	//	merged.show();
		javaDF = javaDF.drop("unique_row_id");
		rDF = rDF.drop("unique_row_id");
		javaColumns = javaDF.columns();
		rColumns = rDF.columns();
		
		List<String> compareColumns = new ArrayList<String>();
		
		for(int i = 0; i < javaColumns.length; i++) {
	
			String compColumn = (javaColumns[i] + "="+rColumns[i]).replaceAll("\\.", "_");
			
			String col1 = normalizeNames(javaColumns[i]);
			String col2 = normalizeNames(rColumns[i]);
			compareColumns.add(col1 );
			compareColumns.add( col2);
			compareColumns.add( compColumn);
//			System.out.println(String.format("Comparing %s with %s", col1, col2));
			
			merged = merged.withColumn(compColumn, when(not(merged.col(col1).eqNullSafe(merged.col(col2))),"not matched").otherwise("matched"));
		}
		
//		

	    Column [] cols = compareColumns.stream().map(x->col(x)).collect(Collectors.toList()).toArray(new Column[0]); 
	    merged.select(cols).show();
		merged.select(cols).write().mode(SaveMode.Overwrite).option("header", true).option("delimiter", ",")
				.csv("/home/hadoop/Downloads/DIFFERENCE");
	}

}
