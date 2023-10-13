package io.auton8.spark.fileInput;

import java.util.List;

public class FileColumn {
	private String columnName;
	private String aliasName;
	private List<FileRule> rules;

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public List<FileRule> getRules() {
		return rules;
	}

	public void setRules(List<FileRule> rules) {
		this.rules = rules;
	}

	public String getAliasName() {
		return aliasName;
	}

	public void setAliasName(String aliasName) {
		this.aliasName = aliasName;
	}

}
