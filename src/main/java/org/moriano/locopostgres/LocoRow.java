package org.moriano.locopostgres;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Represents each of the Rows returned for a BACKEND_DATA_ROW packet
 */
public class LocoRow {

    private int totalColumns;

    /**
     * The value of each column, notice that null columns will contain a null within the list.
     */
    private List<byte[]> columnValues;

    private LocoRow(int totalColumns, List<byte[]> columnValues) {
        this.columnValues = columnValues;
        this.totalColumns = totalColumns;
    }

    public static LocoRow fromPacket(Packet rawPacket) throws SQLException {
        if (rawPacket.getPacketType() != PacketType.BACKEND_DATA_ROW) {
            throw new SQLException("Trying to read backed data row from wrong packet! packet was " + rawPacket);
        }
        List<byte[]> values = new ArrayList<>();

        /**
         * First byte is the id for the packet
         * Then 4 bytes for an int32 indicating the size
         * Then 2 bytes indicating the number of fields, which could be zero
         * Then for each pair
         *      4 bytes with the length of the column value, can be zero. A special case is when it is
         *      -1 in which case a NULL column value is expected and no extra bytes follow
         *      N Bytes with the actual value of the column
         */
        byte[] rawBytes = rawPacket.getPacketContents();
        int numberOfColumns = ByteUtil.getInt16(Arrays.copyOfRange(rawBytes, 5, 7));
        int byteIdx = 7;
        for (int i = 0; i<numberOfColumns; i++) {
            int columnSize = ByteUtil.getInt32(Arrays.copyOfRange(rawBytes, byteIdx, byteIdx+4));
            byteIdx += 4;
            if (columnSize == -1) {
                values.add(null);
            } else {
                byte[] columnValue = Arrays.copyOfRange(rawBytes, byteIdx, byteIdx + columnSize);
                byteIdx += columnSize;
                values.add(columnValue);
            }
        }

        return new LocoRow(numberOfColumns, values);
    }

    public int getTotalColumns() {
        return totalColumns;
    }

    public List<byte[]> getColumnValues() {
        return columnValues;
    }
}
