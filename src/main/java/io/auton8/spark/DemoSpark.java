
package io.auton8.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import io.auton8.spark.utility.Pair;

import static org.apache.spark.sql.functions.*;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.spark.sql.Column;

@FunctionalInterface
interface ApplyInterface<T> {
	public T apply(T input);
}

@FunctionalInterface
interface TransformInterface<T> {
	public T transform(T input, Map<String, Object> parameters);
}

@FunctionalInterface
interface ReplaceInterface<T> {
	public T transform(T input, String replaceValues, String ... values );
}


public class DemoSpark {
	public static SparkSession getSparkSession() {
		return SparkSession.builder().appName("Customer Aggregation pipeline").master("local").getOrCreate();
	}

	public static Dataset<Row> replaceValue(Dataset<Row> df, String originalColumn, String aliasColumn,
			ApplyInterface<Column> tai, String thenValue) {
		return df.withColumn(aliasColumn,
				when(tai.apply(df.col(originalColumn)), thenValue).otherwise(col(originalColumn)));
	}

	public static Dataset<Row> transformValue(Dataset<Row> df, String originalColumn, String aliasColumn,
			TransformInterface<Column> ti, Map<String, Object> params) {
		return df.withColumn(aliasColumn, ti.transform(col(originalColumn), params));
	}

	public static Dataset<Row> createAlias(Dataset<Row> df, String originalColumn, String aliasColumn) {
		return df.withColumn(aliasColumn, col(originalColumn));
	}

	public static Dataset<Row> concatColumns(Dataset<Row> df, String aliasColumn, String column1, String column2,
			String middleString, String endString) {
		return df.withColumn(aliasColumn, concat(when(col(column1).isNull(), "").otherwise(col(column1)),
				lit(middleString), when(col(column2).isNull(), "").otherwise(col(column2)), lit(endString)));
	}

	public static Dataset<Row> createDefaultColumn(Dataset<Row> df, String aliasColumn, String defaultValue) {
		return df.withColumn(aliasColumn, lit(defaultValue));
	}

	@SafeVarargs
	public static Dataset<Row> modifyDate(Dataset<Row> df, String originalColumn, String aliasColumn,
			Pair<String, String>... formats) {
		Column[] cols = new Column[formats.length];

		int index = 0;
		for (Pair<String, String> pair : formats) {			
			cols[index++] = date_format(to_timestamp(col(originalColumn), pair.getKey()), pair.getValue());
		}

		return df.withColumn(aliasColumn, coalesce(cols));
	}

	public static Dataset<Row> replaceRegex(Dataset<Row> df, String originalColumn, String aliasColumn, String findReg, String replaceValue) {
		return df.withColumn(aliasColumn, regexp_replace(col(originalColumn), findReg, replaceValue));
	}
	
	public static Dataset<Row> mapValuesInColumn(Dataset<Row> df, String originalColumn, String aliasColumn, Map<String, String> valueMap) {
		
		StringBuffer buffer = new StringBuffer();
		buffer.append(" case ");
		for(String key : valueMap.keySet()) {
			buffer.append("when ").append(originalColumn).append(" = ").append("'").append(key).append("' then '").append(valueMap.get(key)).append("' ");
		}		
		buffer.append("else null end");
		
		return df.withColumn(aliasColumn, expr(buffer.toString()));
	}
	
	
	public static Dataset<Row> mapFromExcelFile(Dataset<Row> df, String excelSheetPath, String sheetName,
			String keyColumn, String valueColumn, String joinColumn, String aliasColumn)
			throws InvalidFormatException, IOException {
		ExcelReader excelReader = new ExcelReader();

		Dataset<Row> sectorDF = excelReader.readDatasetFromExcel(getSparkSession(), Path.of(excelSheetPath), sheetName,
				keyColumn, valueColumn);
		
		boolean found = false;
		boolean foundKey = false;
		
		for (String columnName : df.columns()) {
			if (columnName.equals(aliasColumn)) {
				found = true;	
			}
			if(columnName.equals(keyColumn)) {
				foundKey = true;
			}
			
			if(found && foundKey)
				break;
		}
		
		if(foundKey) {
			sectorDF = sectorDF.withColumnRenamed(keyColumn, keyColumn+"1");
			keyColumn += "1";
		}
		
		df = df.join(sectorDF, sectorDF.col(keyColumn).eqNullSafe(df.col(joinColumn)),"left");



		if (found) {
			df = df.drop(aliasColumn);
		}
		df = df.withColumnRenamed(valueColumn, aliasColumn);
		return df;
	}

	private static TransformInterface<Column> substringLambda = (Column column, Map<String, Object> params) -> {
		return substring(column, (Integer) params.get("start"), (Integer) params.get("length"));
	};

	private static void transformData() throws InvalidFormatException, IOException, CsvException {

		long start = System.currentTimeMillis();

		Dataset<Row> df = getSparkSession().read().option("header", true).option("delimiter", "|")
				.csv("/home/hadoop/Downloads/CUSTOM.20230409.txt"); 
		df = replaceValue(df, "`SHORT.NAME*`", "SHORT.NAME", (Column col) -> col.isNull(), "COMPANY");

		Map<String, Object> paramMap = new HashMap<String, Object>();
		paramMap.put("start", 0);
		paramMap.put("length", 70);
		df = transformValue(df, "`NAME.1*`", "NAME.1", substringLambda, paramMap);

		paramMap.put("start", 70);
		paramMap.put("length", 70);
		df = transformValue(df, "`NAME.2*`", "NAME.2", substringLambda, paramMap);

		df = createAlias(df, "MAILADD1", "STREET");
		df = createAlias(df, "`TOWN.COUNTRY*`", "TOWN.COUNTRY");
		df = createAlias(df, "MAILPOST", "POST.CODE");
		df = createAlias(df, "COUNTRY*", "COUNTRY");
		df = mapFromExcelFile(df, "/home/hadoop/Downloads/DMS - CUSTOMER 04092023.xlsx", "Sector Mapping",
				"Ultracts MEMBER TYPE", "Transact SECTOR", "`MEMBER TYPE`", "SECTOR");
		df = mapFromExcelFile(df, "/home/hadoop/Downloads/DMS - CUSTOMER 04092023.xlsx", "Account.officer mapping",
				"Ultracs BRANCH", "Transact DEPT.ACCT.OFFICER", "BRANCH", "ACCOUNT.OFFICER");
		df = concatColumns(df, "LEGAL.ID.1::LEGAL.ID.2", "ACN", "ABN", "::", "");
		df = createDefaultColumn(df, "LEGAL.DOC.NAME.1::LEGAL.DOC.NAME.2", "AUS.COMPANY.NO::AUS.BUSINESS.NO");
		df = replaceValue(df, "`TITLE`", "TITLE", (Column col) -> lower(col).isin("master", "det", "insp"), "");
		df = replaceValue(df, "`GENDER`", "GENDER", (Column col) -> lower(col).eqNullSafe("u"), "");
		df = modifyDate(df, "`DATE.OF.BIRTH`", "DATE.OF.BIRTH", Pair.createPair("yyyyddMM", "yyyyMMdd"));
		df = replaceRegex(df, "`EMAIL.1`","EMAIL.1"," ","");
		df = createDefaultColumn(df, "ADDR.LOCATION", "PRIMARY");
		df = modifyDate(df, "DATEMEMBERBEGAN", "CUSTOMER.SINCE", Pair.createPair("MM-dd-yyyy", "yyyyMMdd"));
		df = createDefaultColumn(df, "CUSTOMER.TYPE", "ACTIVE");
		df = mapFromExcelFile(df, "/home/hadoop/Downloads/DMS - CUSTOMER 04092023.xlsx", "Domicile",
				"NONRESCODE", "Transact DOMICILE", "NONRESCODE", "DOMICILE");
		df = createDefaultColumn(df, "ADDRESS.TYPE", "MLTO");		
		df = replaceValue(df, "`MailSTAT`", "COUNTRY.SUBDIVISION",
				(Column col) -> not(upper(col).isin("ACT", "NSW", "NT", "QLD", "SA", "TAS", "VIC", "WA")), "");
		df = mapValuesInColumn(df, "TFNEXEMPTCODE","TFN.EXEM.CATEG",Map.of("1", "444444441", "2", "444444442", "3", "333333333", "4", "000000000", "5", "555555555","6","777777777","7","888888888"));
		df = createDefaultColumn(df, "LEGAL.ISS.DATE.1::LEGAL.ISS.DATE.2", "19990101::19990101");
		df = createDefaultColumn(df, "ADDRESS.COUNTRY","AU");
		
		List<String> columnNames = new ArrayList<>();
		
	    try (Reader reader = Files.newBufferedReader(Path.of("/home/hadoop/Downloads/columnnames.csv"))) {
	        try (CSVReader csvReader = new CSVReader(reader)) {
	        	List<String[]> lines = csvReader.readAll();
	        	
	        	for(String [] line : lines) {
	        		columnNames.add(line[0]);
	        	}
	        }
	    }
	    
	    Column [] cols = columnNames.stream().map(x->col(x)).collect(Collectors.toList()).toArray(new Column[0]); 
	    df.select(cols).write().mode(SaveMode.Overwrite).option("header", true).option("delimiter", ",")
				.csv("/home/hadoop/Downloads/CUSTOMER");
	    
	    long end = System.currentTimeMillis();
	    
	    System.out.println((end - start));
		
	}

	public static void main(String[] args) throws InvalidFormatException, IOException, CsvException {

		transformData();
	}

}
