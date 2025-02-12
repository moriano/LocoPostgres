package org.moriano.locopostgres;

/**
 * Represents a server parameter status.
 *
 * The server parameter status are just a parameter name and a parameter value, both being strings
 */
public class ParameterStatus {

    private final String name;
    private final String value;

    public ParameterStatus(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ParameterStatus that = (ParameterStatus) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        return value != null ? value.equals(that.value) : that.value == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ParameterStatus{" +
                "name='" + name + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
