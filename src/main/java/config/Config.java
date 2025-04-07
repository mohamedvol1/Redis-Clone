package config;

import java.util.HashMap;
import java.util.Map;

public class Config {
	private Map<String, String> configMap;
	
	public Config(String[] args) {
		configMap = new HashMap<>();
		parseConfig(args);
	}
	
	private void parseConfig(String[] args) {
		for (int i = 0; i < args.length; i += 2) {
			if ( i + 1 < args.length) {
				String flag = args[i];
				String value = args[i + 1];
				if ("--dir".equals(flag)) {
					configMap.put("dir", value);
				} else if ("--dbfilename".equals(flag)) {
					configMap.put("dbfilename", value);
				}
			}
			
			// default values
			configMap.putIfAbsent("dir", "/tmp/redis-files");
			configMap.putIfAbsent("dbfilename", "dump.rdb");
		}
	}
	
	public String get(String param) {
		return configMap.get(param);
	}

}
