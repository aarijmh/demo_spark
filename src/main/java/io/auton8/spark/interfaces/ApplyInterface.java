package io.auton8.spark.interfaces;

@FunctionalInterface
public interface ApplyInterface<T> {
	public T apply(T input);
}
