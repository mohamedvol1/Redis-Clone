package streams;

import java.util.LinkedHashMap;
import java.util.Map;

public class Stream {
    private final Map<String, StreamEntry> entries;

    public Stream() {
        entries = new LinkedHashMap<>();
    }

    public String addEntry(StreamEntry entry) {
        validateEntryId(entry.id());
        entries.put(entry.id(), entry);
        return entry.id();
    }

    public Map<String, StreamEntry> getEntries() {
        return entries;
    }

    public String getLastEntryId() {
        if (entries.isEmpty()) {
            return "0-0";
        }

        return entries.keySet().stream().skip(entries.size() - 1).findFirst().orElse("0-0");
    }

    private long[] parseEntryId(String entryId) {
        String[] parts = entryId.split("-");
        try {
            return new long[]{Long.parseLong(parts[0]), Long.parseLong(parts[1])};
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("ERR Invalid stream ID specified: " + entryId);
        }
    }

    private int compareIDs(String id1, String id2) {
        long[] id1Parts = parseEntryId(id1);
        long[] id2Parts = parseEntryId(id2);

        // compare time first if different
        if (id1Parts[0] != id2Parts[0]) {
            return Long.compare(id1Parts[0], id2Parts[0]);
        }

        // if they are equal, compare sequence numbers
        return Long.compare(id1Parts[1], id2Parts[1]);
    }

    public void validateEntryId(String id) {

        // id must be bigger than 0-0
        if (compareIDs(id, "0-1") < 0) {
            throw new IllegalArgumentException("ERR The ID specified in XADD must be greater than 0-0");
        }

        String lastId = getLastEntryId();
        if (compareIDs(id, lastId) <= 0) {
            throw new IllegalArgumentException("ERR The ID specified in XADD is equal or smaller than the target stream top item");
        }
    }
}
