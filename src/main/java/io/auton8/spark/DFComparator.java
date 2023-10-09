package io.auton8.spark;

import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
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
		Dataset<Row> javaDF = getSparkSession().read().option("header", true).option("delimiter", ",")
				.csv("/home/hadoop/Downloads/CUSTOMER/part-00000-185f444c-fc3f-45f7-9d5d-d22e4b9b396e-c000.csv");
		System.out.println(javaDF.count());
		
		Dataset<Row> rDF = getSparkSession().read().option("header", true).option("delimiter", "|")
				.csv("/home/hadoop/Downloads/CUSTOMER.AUTON8.202321091251_header.txt");
		System.out.println(rDF.count());

		javaDF = javaDF.withColumnRenamed("UPLOAD.COMPANY", "UPLOADCOMPANY");
		rDF = rDF.withColumnRenamed("UPLOAD.COMPANY", "UPLOADCOMPANY");
		String [] javaColumns = javaDF.columns();
		String [] rColumns = rDF.columns();
		List<String> compareColumns = new ArrayList<String>();
		
		for(int i = 0; i < javaColumns.length; i++) {
	
			compareColumns.add(javaColumns[i] + "=="+rColumns[i]);
			String col1 = normalizeNames(javaColumns[i]);
			String col2 = normalizeNames(rColumns[i]);
			System.out.println(String.format("Comparing %s with %s", col1, col2));
			javaDF.withColumn(compareColumns.get(i), when(not(javaDF.col(col1).eqNullSafe(rDF.col(col2))),"matched").otherwise("not matched"));
		}
		
	    Column [] cols = compareColumns.stream().map(x->col(x)).collect(Collectors.toList()).toArray(new Column[0]); 
		javaDF.select(cols).write().mode(SaveMode.Overwrite).option("header", true).option("delimiter", ",")
				.csv("/home/hadoop/Downloads/DIFFERENCE");
	}

}
