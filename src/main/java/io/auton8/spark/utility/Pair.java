package io.auton8.spark.utility;

public class Pair <K,V>{

	private K key;
	private V value;
	
	private Pair(K k, V v) {
		this.key = k;
		this.value = v;
	}
	
	public static< K,V>  Pair<K,V> createPair(K k, V v){
		return new Pair<K,V>(k,v);
	}
	
	public K getKey() {
		return key;
	}
	public void setKey(K key) {
		this.key = key;
	}
	public V getValue() {
		return value;
	}
	public void setValue(V value) {
		this.value = value;
	}
	
	
}
