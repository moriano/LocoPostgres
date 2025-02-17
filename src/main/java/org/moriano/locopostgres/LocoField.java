package org.moriano.locopostgres;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * The definition of a field in a response.
 *
 * This pretty much mirrors the data in the BACKEND_ROW_DESCRIPTION package
 */
public class LocoField {

    private String name;
    private int columnObjectId;
    private int columnAttributeNumber;
    private int objectId;
    private int dataTypeSize;
    private int typeModifyier;
    private int formatCode;

    private int fieldPosition;

    public LocoField(String name, int columnObjectId, int columnAttributeNumber, int objectId, int dataTypeSize, int typeModifyier, int formatCode, int fieldPosition) {
        this.name = name;
        this.columnObjectId = columnObjectId;
        this.columnAttributeNumber = columnAttributeNumber;
        this.objectId = objectId;
        this.dataTypeSize = dataTypeSize;
        this.typeModifyier = typeModifyier;
        this.formatCode = formatCode;
        this.fieldPosition = fieldPosition;
    }

    public static List<LocoField> fromPacket(Packet packet, int numberOfFields) {
        List<LocoField> results = new ArrayList<>();
        /*
        Packet contents are

        String with field name
        int32   columnObjectId
        int16   columnAttributeNumber
        int32   objectId
        int16   dataTypeSize
        int32   typeModifyier
        int32   formatCode
         */
        byte[] rawBytes = packet.getPacketContents();

        /*
        The first byte we care about is 8 because
        1 byte for id
        4 bytes for packet size
        2 bytes for number of fields
         */
        int firstRelevantByte = 7;

        for(int i = 0; i<numberOfFields; i++) {
            List<Byte> fieldNameInBytes = new ArrayList<>();
            int byteIdx = firstRelevantByte;
            byte relevantByte = rawBytes[byteIdx]; // 0x07  0x24   0x40 0x60 0x7F
            fieldNameInBytes.add(relevantByte);
            do {
                byteIdx++;
                relevantByte = rawBytes[byteIdx];
                if (relevantByte != 0x00) {
                    fieldNameInBytes.add(relevantByte);
                }
            } while(relevantByte != 0x00);
            byteIdx++; //We move the cursor one position as we are not interested on byte 0x00
            byte[] fieldNameArray = new byte[fieldNameInBytes.size()];
            for(int aux = 0; aux<= fieldNameInBytes.size()-1; aux++) {
                fieldNameArray[aux] = fieldNameInBytes.get(aux);
            }
            String fieldName = new String(fieldNameArray, StandardCharsets.UTF_8);

            int tableId = ByteUtil.getInt32(Arrays.copyOfRange(rawBytes, byteIdx, byteIdx+4));
            byteIdx = byteIdx + 4; // 16469 0x4055

            int columnNumber = ByteUtil.getInt16(Arrays.copyOfRange(rawBytes, byteIdx, byteIdx+2));
            byteIdx = byteIdx + 2;

            int dataTypeId = ByteUtil.getInt32(Arrays.copyOfRange(rawBytes, byteIdx, byteIdx+4));
            byteIdx = byteIdx + 4;

            int dataTypeSize = ByteUtil.getInt16(Arrays.copyOfRange(rawBytes, byteIdx, byteIdx+2));
            byteIdx = byteIdx + 2;

            int typeModifier = ByteUtil.getInt32(Arrays.copyOfRange(rawBytes, byteIdx, byteIdx+4));
            byteIdx = byteIdx + 4;

            int formatCode = ByteUtil.getInt16(Arrays.copyOfRange(rawBytes, byteIdx, byteIdx+2));
            byteIdx = byteIdx + 2;

            results.add(new LocoField(fieldName, tableId, columnNumber, dataTypeId,
                    dataTypeSize, typeModifier, formatCode, i));

            firstRelevantByte = byteIdx;
        }
        return results;
    }

    /**
     * As per the documentation. Zero means text, One means binary. In a RowDescription returned from the statement
     * variant of DESCRIBE, the format code is not yet known and will always be zero.
     * @return
     */
    public boolean isBinaryFormat() {
        return this.formatCode == 1;
    }

    public int getFieldPosition() {
        return fieldPosition;
    }

    public String getName() {
        return name;
    }

    public int getColumnObjectId() {
        return columnObjectId;
    }

    public int getColumnAttributeNumber() {
        return columnAttributeNumber;
    }

    public int getObjectId() {
        return objectId;
    }

    public int getDataTypeSize() {
        return dataTypeSize;
    }

    public int getTypeModifyier() {
        return typeModifyier;
    }

    public int getFormatCode() {
        return formatCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocoField locoField = (LocoField) o;
        return columnObjectId == locoField.columnObjectId &&
                columnAttributeNumber == locoField.columnAttributeNumber &&
                objectId == locoField.objectId &&
                dataTypeSize == locoField.dataTypeSize &&
                typeModifyier == locoField.typeModifyier &&
                formatCode == locoField.formatCode &&
                Objects.equals(name, locoField.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, columnObjectId, columnAttributeNumber, objectId, dataTypeSize, typeModifyier, formatCode);
    }
}
