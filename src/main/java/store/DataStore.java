package store;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class DataStore {
	private final Map<String, Entry> store;
	private final Random random;

	public DataStore() {
		this.store = new HashMap<>();
		this.random = new Random();
	}

	public void set(String key, Object value, long expiryTimeInMillis) {
		long expiryTime = System.currentTimeMillis() + expiryTimeInMillis;
		store.put(key, new Entry(value, expiryTime));
	}

	// overload to set expire time to default = -1
	public void set(String key, Object value) {
		store.put(key, new Entry(value));
	}

	public Object get(String key) {
		Entry entry = store.get(key);
		if (entry == null) {
			return null;
		}

		if (entry.isExpired()) {
			store.remove(key);
			return null;
		}

		return entry.getValue();
	}

	public void activeExpiryCycle(int sampleSize, int threshold) {
		if (store.isEmpty()) {
			return;
		}

		Set<Map.Entry<String, Entry>> entries = store.entrySet();
		@SuppressWarnings("unchecked")
		Map.Entry<String, Entry>[] entryArray = entries.toArray(new Map.Entry[0]);
		int expired = 0;
		int sampled = 0;

		for (int i = 0; i < sampleSize && i < entryArray.length; i++) {
			int index = random.nextInt(entryArray.length);
			Map.Entry<String, Entry> entry = entryArray[index];
			if (entry.getValue().isExpired()) {
				store.remove(entry.getKey());
				expired++;
			}

			sampled++;
		}

		if (sampled > 0 && (expired * 100 / sampled) > threshold) {
			System.out.println("High expiration rate detected: " + expired + "/" + sampled);
			// Could trigger additional cleanup here, but we'll keep it simple for now
		}
	}

}
