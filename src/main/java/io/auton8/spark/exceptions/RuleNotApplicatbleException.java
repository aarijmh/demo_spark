package io.auton8.spark.exceptions;

import java.io.IOException;

public class RuleNotApplicatbleException extends IOException{

	private static final long serialVersionUID = 1L;

	public RuleNotApplicatbleException(String message) {
		super(message);
	}
}
