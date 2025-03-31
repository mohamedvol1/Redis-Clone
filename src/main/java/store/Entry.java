package store;

public class Entry {
	private Object value;
	private final long expiryTimeInMillis;

	public Entry(Object value, long expiryTimeInMillis) {
		this.value = value;
		this.expiryTimeInMillis = expiryTimeInMillis;
	}

	public Entry(Object value) {
		this.value = value;
		this.expiryTimeInMillis = -1;
	}

	public Object getValue() {
		return value;
	}

	public long getExpiryTimeInMillis() {
		return expiryTimeInMillis;
	}

	public boolean isExpired() {
		return expiryTimeInMillis != -1 && System.currentTimeMillis() > expiryTimeInMillis;
	}

}
