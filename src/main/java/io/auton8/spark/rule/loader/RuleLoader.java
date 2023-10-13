package io.auton8.spark.rule.loader;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

import io.auton8.spark.rule.AliasRule;
import io.auton8.spark.rule.ConcatColumnRule;
import io.auton8.spark.rule.DefaultColumnRule;
import io.auton8.spark.rule.IRule;
import io.auton8.spark.rule.MapFromExcelFileRule;
import io.auton8.spark.rule.ModifyDateRule;
import io.auton8.spark.rule.ReplaceRegexRule;
import io.auton8.spark.rule.ReplaceRule;
import io.auton8.spark.rule.TransformRule;


public class RuleLoader {
	
	private static Map<String, IRule> ruleMap = new HashMap<String, IRule>();
	
	static {
		loadRules();
	}
	
	private RuleLoader() {
		
	}

	static private void loadRules() {
		ServiceLoader<IRule> loader = ServiceLoader.load(IRule.class);
		long count = StreamSupport.stream(loader.spliterator(), false).count();
		if (count == 1) {
			System.out.println("Hoorah");
		}
		
		ruleMap.put("aliasRule", new AliasRule());
		ruleMap.put("concatRule", new ConcatColumnRule());
		ruleMap.put("defaultRule", new DefaultColumnRule());
		ruleMap.put("mapFromExcelFileRule", new MapFromExcelFileRule());
		ruleMap.put("modifyDataRule", new ModifyDateRule());
		ruleMap.put("replaceRegexRule", new ReplaceRegexRule());
		ruleMap.put("replaceRule", new ReplaceRule());
		ruleMap.put("transformRule", new TransformRule());
	}
	
	public static Map<String, IRule> getRuleMap(){
		return ruleMap;
	}
}
