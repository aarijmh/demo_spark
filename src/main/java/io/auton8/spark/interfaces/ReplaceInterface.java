package io.auton8.spark.interfaces;

@FunctionalInterface
public interface ReplaceInterface<T> {
	public T transform(T input, String replaceValues, String ... values );
}

