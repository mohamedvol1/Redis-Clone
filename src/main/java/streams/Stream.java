package streams;

import java.util.LinkedHashMap;
import java.util.Map;

public class Stream {
    private final Map<String, StreamEntry> entries;

    public Stream() {
        entries = new LinkedHashMap<>();
    }

    public String addEntry(StreamEntry entry) {
        entries.put(entry.id(), entry);
        return entry.id();
    }

    public Map<String, StreamEntry> getEntries() {
        return entries;
    }
}
