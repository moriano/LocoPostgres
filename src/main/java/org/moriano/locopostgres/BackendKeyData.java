package org.moriano.locopostgres;

/**
 * This contains the required information to cancel queries for a given connection.
 *
 * The values of this object are populated during the connection establishment once a BACKEND_KEY_DATA packet is
 * received.
 */
public class BackendKeyData {

    private final int processId;
    private final int secretKey;

    public BackendKeyData(int processId, int secretKey) {
        this.processId = processId;
        this.secretKey = secretKey;
    }

    public int getProcessId() {
        return processId;
    }

    public int getSecretKey() {
        return secretKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BackendKeyData that = (BackendKeyData) o;

        if (processId != that.processId) return false;
        return secretKey == that.secretKey;
    }

    @Override
    public int hashCode() {
        int result = processId;
        result = 31 * result + secretKey;
        return result;
    }

    @Override
    public String toString() {
        return "BackendKeyData{" +
                "processId=" + processId +
                ", secretKey=" + secretKey +
                '}';
    }
}
