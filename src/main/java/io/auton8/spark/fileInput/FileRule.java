package io.auton8.spark.fileInput;

import java.util.Map;

public class FileRule {
	private String ruleName;
	private Map<String, Object> params;
	public String getRuleName() {
		return ruleName;
	}
	public void setRuleName(String ruleName) {
		this.ruleName = ruleName;
	}
	public Map<String, Object> getParams() {
		return params;
	}
	public void setParams(Map<String, Object> params) {
		this.params = params;
	}
	
	
}
