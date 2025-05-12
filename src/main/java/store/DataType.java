package store;

public enum DataType {
    NONE,
    STRING,
    STREAM;

    @Override
    public String toString() {
        return name().toLowerCase();
    }


}
