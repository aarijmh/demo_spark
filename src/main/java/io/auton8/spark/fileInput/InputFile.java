package io.auton8.spark.fileInput;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InputFile {
	
	
	private String fileName;
	private Map<String, String> fileOptions = new HashMap<>();
	private List<FileColumn> columns;
	
	
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public Map<String, String> getFileOptions() {
		return fileOptions;
	}
	public void setFileOptions(Map<String, String> fileOptions) {
		this.fileOptions = fileOptions;
	}
	public List<FileColumn> getColumns() {
		return columns;
	}
	public void setColumns(List<FileColumn> columns) {
		this.columns = columns;
	}
	
	

}
