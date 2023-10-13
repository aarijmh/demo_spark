package io.auton8.spark.interfaces;

import java.util.Map;

@FunctionalInterface
public interface TransformInterface<T> {
	public T transform(T input, Map<String, Object> parameters);
}