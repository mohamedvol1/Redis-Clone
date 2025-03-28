package store;

import java.util.HashMap;
import java.util.Map;

public class DataStore {
	private final Map<String, Object> store;
	
	public DataStore() {
		this.store = new HashMap<>();
	}
	
	public void set(String key, Object value) {
		store.put(key, value);
	}
	
	public Object get(String key) {
		return store.get(key);
	}

}
