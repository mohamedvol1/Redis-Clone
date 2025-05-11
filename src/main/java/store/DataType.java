package store;

public enum DataType {
    NONE,
    STRING;

    @Override
    public String toString() {
        return name().toLowerCase();
    }


}
