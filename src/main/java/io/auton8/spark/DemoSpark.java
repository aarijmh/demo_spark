package io.auton8.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.spark.sql.Column;

@FunctionalInterface
interface ApplyInterface<T>{
	public T apply(T input);
}

@FunctionalInterface
interface TransformInterface<T>{
	public T transform(T input, Map<String, Object> parameters);
}

public class DemoSpark {
	public static SparkSession getSparkSession() {
	    return SparkSession.builder()
	      .appName("Customer Aggregation pipeline")
	      .master("local")
	      .getOrCreate();
	}
	
	public static Dataset<Row> replaceValue(Dataset<Row> df , String originalColumn, String aliasColumn, ApplyInterface<Column> tai, String thenValue ){
		return df.withColumn(aliasColumn, when(tai.apply(df.col(originalColumn)), thenValue ).otherwise(col(originalColumn)));
	}
	
	public static Dataset<Row> transformValue(Dataset<Row> df , String originalColumn, String aliasColumn, TransformInterface<Column> ti, Map<String, Object> params){
		return df.withColumn(aliasColumn, ti.transform(col(originalColumn), params));
	}
	
	public static Dataset<Row> createAlias(Dataset<Row> df , String originalColumn, String aliasColumn){
		return df.withColumn(aliasColumn, col(originalColumn));
	}

	private static TransformInterface<Column> substringLambda = (Column column, Map<String, Object> params)  ->{
		return substring(column, (Integer)params.get("start"), (Integer) params.get("length"));
	};
	
	private static void transformData() throws InvalidFormatException, IOException {

		ExcelReader excelReader = new ExcelReader();

		Dataset<Row> df = getSparkSession().read().option("header", true).option("delimiter", "|").csv("/home/hadoop/Downloads/CUSTOM.20230409.txt");
		df = replaceValue(df, "`SHORT.NAME*`", "SHORT.NAME", (Column col)-> col.isNull(),"COMPANY");
		
		Map<String, Object> paramMap = new HashMap<String, Object>();
		paramMap.put("start", 0);
		paramMap.put("length", 70);
		df = transformValue(df, "`NAME.1*`", "NAME.1", substringLambda, paramMap);
		
		paramMap.put("start", 70);
		paramMap.put("length", 70);
		df = transformValue(df, "`NAME.2*`", "NAME.2", substringLambda, paramMap);
		
		df = createAlias(df,"MAILADD1", "STREET");
		df = createAlias(df, "`TOWN.COUNTRY*`","TOWN.COUNTRY");
		df = createAlias(df, "MAILPOST","POST.CODE");
		df = createAlias(df, "COUNTRY*","COUNTRY");
		
		Dataset<Row> sectorDF =  excelReader.readDatasetFromExcel(getSparkSession(), Path.of("/home/hadoop/Downloads/DMS - CUSTOMER 04092023.xlsx"), "Sector Mapping", "Ultracts MEMBER TYPE", "Transact SECTOR");
		df = df.join(sectorDF,sectorDF.col("`Ultracts MEMBER TYPE`").eqNullSafe(df.col("`MEMBER TYPE`")));
		df = df.withColumnRenamed("Transact SECTOR", "SECTOR.1");
		
		Dataset<Row> accountDF =  excelReader.readDatasetFromExcel(getSparkSession(), Path.of("/home/hadoop/Downloads/DMS - CUSTOMER 04092023.xlsx"), "Account.officer mapping", "Ultracs BRANCH", "Transact DEPT.ACCT.OFFICER");
		df = df.join(accountDF,accountDF.col("`Ultracs BRANCH`").eqNullSafe(df.col("BRANCH")));
		df = df.withColumnRenamed("Transact DEPT.ACCT.OFFICER", "ACCOUNT.OFFICER.1");
		
		df = df.withColumn("LEGAL.ID.1::LEGAL.ID.2", concat( when(col("ACN").isNull(), "").otherwise(col("ACN")), lit("::"), when(col("ABN").isNull(),"").otherwise(col("ABN"))));
		df = df.withColumn("LEGAL.DOC.NAME.1::LEGAL.DOC.NAME.2", lit("AUS.COMPANY.NO::AUS.BUSINESS.NO"));
		
		df = df.withColumn("TITLE", when(lower(col("TITLE")).isin("master","det","insp"),""));
		
		df = replaceValue(df, "`MailSTAT`", "COUNTRY.SUBDIVISION", (Column col)-> not(upper(col).isin("ACT","NSW","NT","QLD","SA","TAS","VIC","WA")),"");
		df.write().mode(SaveMode.Overwrite).option("header", true).option("delimiter", ",").csv("/home/hadoop/Downloads/CUSTOMER");
		df.show();
	}
	public static void main(String[] args) throws InvalidFormatException, IOException {
		
		transformData();
	}

}
