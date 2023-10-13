package io.auton8.spark.utility;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class ExcelReader {
	
	public String getStringCellValue(Cell cell) {
		switch (cell.getCellType()) {
		case NUMERIC:
			return Double.toString(cell.getNumericCellValue()).replace(".0", "");
		case STRING:
			return cell.getStringCellValue();
		default:
			break;
		}
		return null;
	}
	

	
	public Dataset<Row> readDatasetFromExcel(SparkSession session, Path excelFilePath, String sheetName, String keyColumn, String valueColumn) throws InvalidFormatException, IOException{
		try(Workbook workbook = new XSSFWorkbook(excelFilePath.toFile())){
			Sheet sheet = workbook.getSheet(sheetName);
			org.apache.poi.ss.usermodel.Row headerRow = sheet.getRow(0);
			int keyColumnIndex = -1;
			int valueColumnIndex = -1;
			
			for(Cell cell : headerRow) {
				String cellValue = cell.getStringCellValue();
				if(keyColumn.toLowerCase().equalsIgnoreCase(cellValue)) {
					keyColumnIndex = cell.getColumnIndex();
				}
				if(valueColumn.toLowerCase().equalsIgnoreCase(cellValue)) {
					valueColumnIndex = cell.getColumnIndex();
				}
			}
			
			if( keyColumnIndex < 0 || valueColumnIndex < 0) {
				throw new IllegalArgumentException("Index and/or value columns are not configured");
			}
			
			Map<String, Object> paramMap = new HashMap<String, Object>();
			List<Pair<String, Object>> pairList = new ArrayList<Pair<String,Object>>();
			for(org.apache.poi.ss.usermodel.Row row : sheet) {
				if(row.getRowNum() == 0)
					continue;
				pairList.add(Pair.createPair(getStringCellValue(row.getCell(keyColumnIndex)), getStringCellValue(row.getCell(valueColumnIndex))));
				paramMap.put(getStringCellValue(row.getCell(keyColumnIndex)), getStringCellValue(row.getCell(valueColumnIndex)));
			}
			
			StructType schema = Converters.createDataSchemaAllString(keyColumn, valueColumn);
			Dataset<Row> df = Converters.makeDataSetOfRow(session, pairList, schema);

			return df;
		}
	}

}
