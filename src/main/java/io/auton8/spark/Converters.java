package io.auton8.spark;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import io.auton8.spark.utility.Pair;

public class Converters {

	public static StructType createDataSchemaAllString(String ... args) {
		StructField[] structArray = new StructField[args.length];
		int i = 0;
		for(String ar : args) {
			structArray[i++] = DataTypes.createStructField(ar, DataTypes.StringType, true);
		}
	    return DataTypes.createStructType(structArray);
	}
	
	private static MapFunction<Pair<String, Object>, Row> pairMap = (Pair<String, Object> p) ->{
		Row r = RowFactory.create(p.getKey(),p.getValue());
		return r;
	};
	
	public static Dataset<Row> makeDataSetOfRow(SparkSession session, List<Pair<String, Object>> list, StructType structType){
		List<Row> rows = list.stream().map(p -> {
			try {
				return pairMap.call(p);
			} catch (Exception e) {
				e.printStackTrace();
			}
			return null;
		}).collect(Collectors.toList());
		
		return session.createDataFrame(rows, structType);
	}
}
