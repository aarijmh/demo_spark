package io.auton8.spark.fileInput;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InputFile {
	
	
	private String fileName;
	private String saveLocation;
	private String compareLocation;
	private String outputDelimiter;
	private String transformFileFormat;
    private String compareTransformFileFormat;
    private Boolean withHeader;
    private Boolean withoutHeader;
    private String transformFileHeaderFormat;
    private String saveLocationHeader;
    private String baseFolder;
    private String folderSeparator;
    
	private Map<String, String> fileOptions = new HashMap<>();
	private List<FileColumn> columns;
	private Map<String, String> aliases = new HashMap<>();
	
	
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
	public String getSaveLocation() {
		return saveLocation;
	}
	public void setSaveLocation(String saveLocation) {
		this.saveLocation = saveLocation;
	}
	public String getCompareLocation() {
		return compareLocation;
	}
	public void setCompareLocation(String compareLocation) {
		this.compareLocation = compareLocation;
	}
	public String getOutputDelimiter() {
		return outputDelimiter;
	}
	public void setOutputDelimiter(String outputDelimiter) {
		this.outputDelimiter = outputDelimiter;
	}
	public String getTransformFileFormat() {
		return transformFileFormat;
	}
	public void setTransformFileFormat(String transformFileFormat) {
		this.transformFileFormat = transformFileFormat;
	}
	public String getCompareTransformFileFormat() {
		return compareTransformFileFormat;
	}
	public void setCompareTransformFileFormat(String compareTransformFileFormat) {
		this.compareTransformFileFormat = compareTransformFileFormat;
	}
	public Boolean getWithHeader() {
		return withHeader;
	}
	public void setWithHeader(Boolean withHeader) {
		this.withHeader = withHeader;
	}
	public Boolean getWithoutHeader() {
		return withoutHeader;
	}
	public void setWithoutHeader(Boolean withoutHeader) {
		this.withoutHeader = withoutHeader;
	}
	public String getTransformFileHeaderFormat() {
		return transformFileHeaderFormat;
	}
	public void setTransformFileHeaderFormat(String transformFileHeaderFormat) {
		this.transformFileHeaderFormat = transformFileHeaderFormat;
	}
	public String getSaveLocationHeader() {
		return saveLocationHeader;
	}
	public void setSaveLocationHeader(String saveLocationHeader) {
		this.saveLocationHeader = saveLocationHeader;
	}
	public Map<String, String> getAliases() {
		return aliases;
	}
	public void setAliases(Map<String, String> aliases) {
		this.aliases = aliases;
	}
	public String getBaseFolder() {
		return baseFolder;
	}
	public void setBaseFolder(String baseFolder) {
		this.baseFolder = baseFolder;
	}
	public String getFolderSeparator() {
		return folderSeparator;
	}
	public void setFolderSeparator(String folderSeparator) {
		this.folderSeparator = folderSeparator;
	}
	
	

}
