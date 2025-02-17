package org.moriano.locopostgres;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Map;

/**
 * The class providing a a ResultSet implementation.
 *
 * In postgres when a query is executed, we need to first read
 */
public class LocoResultSet implements ResultSet {

    private LocoNetwork locoNetwork;
    private LocoRowDescription locoRowDescription;
    private Packet lastPacketFromServer;
    private boolean empty = false;

    public LocoResultSet(LocoNetwork locoNetwork, LocoRowDescription locoRowDescription) {
        this.locoNetwork = locoNetwork;
        this.locoRowDescription = locoRowDescription;
        if (locoNetwork == null && locoRowDescription == null) {
            this.empty = true;
        }
    }

    public static LocoResultSet emptyResultSet() {
        return new LocoResultSet(null, null);
    }

    @Override
    public boolean next() throws SQLException {
        if (this.empty) {
            return false;
        }
        this.lastPacketFromServer = this.locoNetwork.readFromServer();
        if (this.lastPacketFromServer.getPacketType() == PacketType.BACKEND_COMMAND_COMPLETE) {
            return false;
        } else if (this.lastPacketFromServer.getPacketType() != PacketType.BACKEND_DATA_ROW) {
            this.lastPacketFromServer = this.locoNetwork.readUntilPacketType(PacketType.BACKEND_DATA_ROW);
        }
        return true;
    }

    @Override
    public void close() throws SQLException {
        // Intentionally empty
    }

    @Override
    public boolean wasNull() throws SQLException {
        LocoRow locoRow = LocoRow.fromPacket(this.lastPacketFromServer);
        int totalColumns = locoRow.getTotalColumns();
        return this.getRawBytes(totalColumns) == null;
    }

    @Override
    public String getString(int i) throws SQLException {
        byte[] rawData = this.getRawBytes(i);
        return rawData == null ? null : new String(rawData);
    }

    @Override
    public boolean getBoolean(int i) throws SQLException {
        byte[] rawData = this.getRawBytes(i);
        if (rawData == null) {
            return false;
        } else if (rawData.length > 1) {
            throw new SQLException("The column cannot be represented as a boolean, value was " + new String(rawData));
        } else {
            char value = (char)rawData[0];
            if (value == 't') {
                return true;
            } else {
                return false;
            }
        }
    }

    @Override
    public byte getByte(int i) throws SQLException {
        byte[] rawData = this.getRawBytes(i);
        return rawData == null ? 0 : rawData[0];
    }

    @Override
    public short getShort(int i) throws SQLException {
        byte[] rawData = this.getRawBytes(i);
        int power = 0;
        short total = 0;
        char[] chars = ByteUtil.asCharArray(rawData);
        for (int idx = chars.length - 1; idx >= 0; idx--) {
            total += Character.getNumericValue(chars[idx]) * Math.pow(10, power);
            power++;
        }
        return total;
    }

    @Override
    public int getInt(int i) throws SQLException {
        byte[] rawData = this.getRawBytes(i);
        int power = 0;
        int total = 0;
        char[] chars = ByteUtil.asCharArray(rawData);
        for (int idx = chars.length - 1; idx >= 0; idx--) {
            total += Character.getNumericValue(chars[idx]) * Math.pow(10, power);
            power++;
        }
        return total;
    }

    @Override
    public long getLong(int i) throws SQLException {
        byte[] rawData = this.getRawBytes(i);
        int power = 0;
        long total = 0;
        char[] chars = ByteUtil.asCharArray(rawData);
        for (int idx = chars.length - 1; idx >= 0; idx--) {
            total += Character.getNumericValue(chars[idx]) * Math.pow(10, power);
            power++;
        }
        return total;
    }

    @Override
    public float getFloat(int i) throws SQLException {
        byte[] rawData = this.getRawBytes(i);
        return Float.valueOf(new String(rawData));
    }

    @Override
    public double getDouble(int i) throws SQLException {
        byte[] rawData = this.getRawBytes(i);
        return Double.valueOf(new String(rawData));
    }

    /**
     * @param i
     * @param i1
     * @deprecated
     */
    @Override
    public BigDecimal getBigDecimal(int i, int i1) throws SQLException {
        throw new UnsupportedOperationException("This is deprecated, either use getBigDecimal(int i) or " +
                "getBigDecimal(String column)");
    }

    @Override
    public byte[] getBytes(int i) throws SQLException {
        byte[] rawBytes = this.getRawBytes(i);
        /*
        Notice! we cannot simply pass the bytes we read from the response as they are.

        IF they are stored as binary bytes then sure we can go ahead and return whatever is in the packet,
        otherwise we need to convert the byte type of postgres to the byte type of java
         */
        LocoField locoField = this.locoRowDescription.getFieldByPosition(i);
        if (locoField.isBinaryFormat()) {
            return rawBytes;
        } else {
            /*
            Lets convert! Binary is expected to follow the format \xSOMEBYTES
             */
            if (rawBytes[0] == '\\' && rawBytes[1] == 'x') {
                /*
                Convert all bytes except the first two to hexadecimal. The way this works is that we will get the
                actual bytes represented as hexadecimal values, this means that we will get things like
                "F0" this means byte[0] is F and byte[1] is 0, we need to put this together as single java byte
                 */
                byte[] result = new byte[(rawBytes.length-2)/2];
                int resultIdx = 0;
                int rawIdx = 2;
                while(rawIdx < rawBytes.length) {
                    char upper = ByteUtil.asChar(rawBytes[rawIdx]);
                    rawIdx++;
                    char lower =  ByteUtil.asChar(rawBytes[rawIdx]);
                    rawIdx++;
                    String merged = new String(new char[]{upper, lower});
                    result[resultIdx] = (byte)Integer.parseInt(merged, 16);
                    resultIdx++;
                }
                return result;
            } else {
                return rawBytes;
            }
        }
    }

    @Override
    public Date getDate(int i) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(int i) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int i) throws SQLException {
        return null;
    }

    @Override
    public InputStream getAsciiStream(int i) throws SQLException {
        return null;
    }

    /**
     * @param i
     * @deprecated
     */
    @Override
    public InputStream getUnicodeStream(int i) throws SQLException {
        return null;
    }

    @Override
    public InputStream getBinaryStream(int i) throws SQLException {
        return null;
    }

    @Override
    public String getString(String s) throws SQLException {
        int position = this.findColumnPosition(s);
        return this.getString(position);
    }


    @Override
    public boolean getBoolean(String s) throws SQLException {
        int position = this.findColumnPosition(s);
        return this.getBoolean(position);
    }

    @Override
    public byte getByte(String s) throws SQLException {
        int position = this.findColumnPosition(s);
        return this.getByte(position);
    }

    @Override
    public short getShort(String s) throws SQLException {
        return this.getShort(this.findColumnPosition(s));
    }

    @Override
    public int getInt(String s) throws SQLException {
        int position = this.findColumnPosition(s);
        return this.getInt(position);
    }

    @Override
    public long getLong(String s) throws SQLException {
        return this.getLong(this.findColumnPosition(s));
    }

    @Override
    public float getFloat(String s) throws SQLException {
        return this.getFloat(this.findColumnPosition(s));
    }

    @Override
    public double getDouble(String s) throws SQLException {
        return this.getDouble(this.findColumnPosition(s));
    }

    /**
     * @param s
     * @param i
     * @deprecated
     */
    @Override
    public BigDecimal getBigDecimal(String s, int i) throws SQLException {
        throw new UnsupportedOperationException("This is deprecated, either use getBigDecimal(int i) or " +
                "getBigDecimal(String column)");
    }

    @Override
    public byte[] getBytes(String s) throws SQLException {
        return this.getBytes(this.findColumnPosition(s));
    }

    @Override
    public Date getDate(String s) throws SQLException {
        byte[] rawDate = this.getRawBytes(this.findColumnPosition(s));
        /*
        This method needs to deal with different cases, specifically it needs to deal with types
        - Date: Where we receive a date in the form yyyy-mm-dd
        - DateTime: where we receive a data and a time in the form of yyyy-dd-mm hh:mm:ss
         */
        String raw = new String(rawDate);
        if (raw.length() == 10) { // Assume we are dealing with yyyy-mm-dd
            LocalDate localDate = LocalDate.parse(raw);
            return new Date(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli());
        }

        LocalDateTime localDateTime = LocalDateTime.parse(new String(rawDate), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        return Date.valueOf(localDateTime.toLocalDate());
    }

    @Override
    public Time getTime(String s) throws SQLException {
        byte[] rawTime = this.getRawBytes(this.findColumnPosition(s));
        Time result = Time.valueOf(LocalTime.parse(new String(rawTime)));
        return result;
    }

    @Override
    public Timestamp getTimestamp(String s) throws SQLException {
        throw new SQLException("getTimestamp is not implemented!. Reveived value was " + s);
    }

    @Override
    public InputStream getAsciiStream(String s) throws SQLException {
        return null;
    }

    /**
     * @param s
     * @deprecated
     */
    @Override
    public InputStream getUnicodeStream(String s) throws SQLException {
        return null;
    }

    @Override
    public InputStream getBinaryStream(String s) throws SQLException {
        return null;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public String getCursorName() throws SQLException {
        return null;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public Object getObject(int i) throws SQLException {
        return null;
    }

    @Override
    public Object getObject(String s) throws SQLException {
        return null;
    }

    @Override
    public int findColumn(String s) throws SQLException {
        return 0;
    }

    @Override
    public Reader getCharacterStream(int i) throws SQLException {
        return null;
    }

    @Override
    public Reader getCharacterStream(String s) throws SQLException {
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(int i) throws SQLException {
        byte[] rawData = this.getRawBytes(i);
        char[] chars = ByteUtil.asCharArray(rawData);
        return new BigDecimal(chars);
    }

    @Override
    public BigDecimal getBigDecimal(String s) throws SQLException {
        return this.getBigDecimal(this.findColumnPosition(s));
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return false;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return false;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return false;
    }

    @Override
    public boolean isLast() throws SQLException {
        return false;
    }

    @Override
    public void beforeFirst() throws SQLException {

    }

    @Override
    public void afterLast() throws SQLException {

    }

    @Override
    public boolean first() throws SQLException {
        return false;
    }

    @Override
    public boolean last() throws SQLException {
        return false;
    }

    @Override
    public int getRow() throws SQLException {
        return 0;
    }

    @Override
    public boolean absolute(int i) throws SQLException {
        return false;
    }

    @Override
    public boolean relative(int i) throws SQLException {
        return false;
    }

    @Override
    public boolean previous() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int i) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int i) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getType() throws SQLException {
        return 0;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return false;
    }

    @Override
    public void updateNull(int i) throws SQLException {

    }

    @Override
    public void updateBoolean(int i, boolean b) throws SQLException {

    }

    @Override
    public void updateByte(int i, byte b) throws SQLException {

    }

    @Override
    public void updateShort(int i, short i1) throws SQLException {

    }

    @Override
    public void updateInt(int i, int i1) throws SQLException {

    }

    @Override
    public void updateLong(int i, long l) throws SQLException {

    }

    @Override
    public void updateFloat(int i, float v) throws SQLException {

    }

    @Override
    public void updateDouble(int i, double v) throws SQLException {

    }

    @Override
    public void updateBigDecimal(int i, BigDecimal bigDecimal) throws SQLException {

    }

    @Override
    public void updateString(int i, String s) throws SQLException {

    }

    @Override
    public void updateBytes(int i, byte[] bytes) throws SQLException {

    }

    @Override
    public void updateDate(int i, Date date) throws SQLException {

    }

    @Override
    public void updateTime(int i, Time time) throws SQLException {

    }

    @Override
    public void updateTimestamp(int i, Timestamp timestamp) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int i, InputStream inputStream, int i1) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int i, InputStream inputStream, int i1) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int i, Reader reader, int i1) throws SQLException {

    }

    @Override
    public void updateObject(int i, Object o, int i1) throws SQLException {

    }

    @Override
    public void updateObject(int i, Object o) throws SQLException {

    }

    @Override
    public void updateNull(String s) throws SQLException {

    }

    @Override
    public void updateBoolean(String s, boolean b) throws SQLException {

    }

    @Override
    public void updateByte(String s, byte b) throws SQLException {

    }

    @Override
    public void updateShort(String s, short i) throws SQLException {

    }

    @Override
    public void updateInt(String s, int i) throws SQLException {

    }

    @Override
    public void updateLong(String s, long l) throws SQLException {

    }

    @Override
    public void updateFloat(String s, float v) throws SQLException {

    }

    @Override
    public void updateDouble(String s, double v) throws SQLException {

    }

    @Override
    public void updateBigDecimal(String s, BigDecimal bigDecimal) throws SQLException {

    }

    @Override
    public void updateString(String s, String s1) throws SQLException {

    }

    @Override
    public void updateBytes(String s, byte[] bytes) throws SQLException {

    }

    @Override
    public void updateDate(String s, Date date) throws SQLException {

    }

    @Override
    public void updateTime(String s, Time time) throws SQLException {

    }

    @Override
    public void updateTimestamp(String s, Timestamp timestamp) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String s, InputStream inputStream, int i) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String s, InputStream inputStream, int i) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String s, Reader reader, int i) throws SQLException {

    }

    @Override
    public void updateObject(String s, Object o, int i) throws SQLException {

    }

    @Override
    public void updateObject(String s, Object o) throws SQLException {

    }

    @Override
    public void insertRow() throws SQLException {

    }

    @Override
    public void updateRow() throws SQLException {

    }

    @Override
    public void deleteRow() throws SQLException {

    }

    @Override
    public void refreshRow() throws SQLException {

    }

    @Override
    public void cancelRowUpdates() throws SQLException {

    }

    @Override
    public void moveToInsertRow() throws SQLException {

    }

    @Override
    public void moveToCurrentRow() throws SQLException {

    }

    @Override
    public Statement getStatement() throws SQLException {
        return null;
    }

    @Override
    public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public Ref getRef(int i) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(int i) throws SQLException {
        return null;
    }

    @Override
    public Clob getClob(int i) throws SQLException {
        return null;
    }

    @Override
    public Array getArray(int i) throws SQLException {
        return null;
    }

    @Override
    public Object getObject(String s, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public Ref getRef(String s) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(String s) throws SQLException {
        return null;
    }

    @Override
    public Clob getClob(String s) throws SQLException {
        return null;
    }

    @Override
    public Array getArray(String s) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(int i, Calendar calendar) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(String s, Calendar calendar) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(int i, Calendar calendar) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(String s, Calendar calendar) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int i, Calendar calendar) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(String s, Calendar calendar) throws SQLException {
        return null;
    }

    @Override
    public URL getURL(int i) throws SQLException {
        return null;
    }

    @Override
    public URL getURL(String s) throws SQLException {
        return null;
    }

    @Override
    public void updateRef(int i, Ref ref) throws SQLException {

    }

    @Override
    public void updateRef(String s, Ref ref) throws SQLException {

    }

    @Override
    public void updateBlob(int i, Blob blob) throws SQLException {

    }

    @Override
    public void updateBlob(String s, Blob blob) throws SQLException {

    }

    @Override
    public void updateClob(int i, Clob clob) throws SQLException {

    }

    @Override
    public void updateClob(String s, Clob clob) throws SQLException {

    }

    @Override
    public void updateArray(int i, Array array) throws SQLException {

    }

    @Override
    public void updateArray(String s, Array array) throws SQLException {

    }

    @Override
    public RowId getRowId(int i) throws SQLException {
        return null;
    }

    @Override
    public RowId getRowId(String s) throws SQLException {
        return null;
    }

    @Override
    public void updateRowId(int i, RowId rowId) throws SQLException {

    }

    @Override
    public void updateRowId(String s, RowId rowId) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void updateNString(int i, String s) throws SQLException {

    }

    @Override
    public void updateNString(String s, String s1) throws SQLException {

    }

    @Override
    public void updateNClob(int i, NClob nClob) throws SQLException {

    }

    @Override
    public void updateNClob(String s, NClob nClob) throws SQLException {

    }

    @Override
    public NClob getNClob(int i) throws SQLException {
        return null;
    }

    @Override
    public NClob getNClob(String s) throws SQLException {
        return null;
    }

    @Override
    public SQLXML getSQLXML(int i) throws SQLException {
        return null;
    }

    @Override
    public SQLXML getSQLXML(String s) throws SQLException {
        return null;
    }

    @Override
    public void updateSQLXML(int i, SQLXML sqlxml) throws SQLException {

    }

    @Override
    public void updateSQLXML(String s, SQLXML sqlxml) throws SQLException {

    }

    @Override
    public String getNString(int i) throws SQLException {
        return null;
    }

    @Override
    public String getNString(String s) throws SQLException {
        return null;
    }

    @Override
    public Reader getNCharacterStream(int i) throws SQLException {
        return null;
    }

    @Override
    public Reader getNCharacterStream(String s) throws SQLException {
        return null;
    }

    @Override
    public void updateNCharacterStream(int i, Reader reader, long l) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(String s, Reader reader, long l) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int i, InputStream inputStream, long l) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int i, InputStream inputStream, long l) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int i, Reader reader, long l) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String s, InputStream inputStream, long l) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String s, InputStream inputStream, long l) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String s, Reader reader, long l) throws SQLException {

    }

    @Override
    public void updateBlob(int i, InputStream inputStream, long l) throws SQLException {

    }

    @Override
    public void updateBlob(String s, InputStream inputStream, long l) throws SQLException {

    }

    @Override
    public void updateClob(int i, Reader reader, long l) throws SQLException {

    }

    @Override
    public void updateClob(String s, Reader reader, long l) throws SQLException {

    }

    @Override
    public void updateNClob(int i, Reader reader, long l) throws SQLException {

    }

    @Override
    public void updateNClob(String s, Reader reader, long l) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(int i, Reader reader) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(String s, Reader reader) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int i, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int i, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int i, Reader reader) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String s, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String s, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String s, Reader reader) throws SQLException {

    }

    @Override
    public void updateBlob(int i, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateBlob(String s, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateClob(int i, Reader reader) throws SQLException {

    }

    @Override
    public void updateClob(String s, Reader reader) throws SQLException {

    }

    @Override
    public void updateNClob(int i, Reader reader) throws SQLException {

    }

    @Override
    public void updateNClob(String s, Reader reader) throws SQLException {

    }

    @Override
    public <T> T getObject(int i, Class<T> aClass) throws SQLException {
        return null;
    }

    @Override
    public <T> T getObject(String s, Class<T> aClass) throws SQLException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> aClass) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        return false;
    }

    private byte[] getRawBytes(String fieldName) throws SQLException {
        LocoField locoField = this.locoRowDescription.getFieldByName(fieldName);
        LocoRow locoRow = LocoRow.fromPacket(this.lastPacketFromServer);
        byte[] rawData = locoRow.getColumnValues().get(locoField.getFieldPosition());
        return rawData;
    }

    private byte[] getRawBytes(int fieldPosition) throws SQLException {
        LocoRow locoRow = LocoRow.fromPacket(this.lastPacketFromServer);
         /*
        We store our fields in a List, which is zero-indexed, however getting a value by an index is 1-indexed,
        so we need to substract 1 to the parameter.
         */
        byte[] rawData = locoRow.getColumnValues().get(fieldPosition - 1);
        return rawData;
    }

    private int findColumnPosition(String columnName) throws SQLException {
        LocoField locoField = this.locoRowDescription.getFieldByName(columnName);
        return locoField.getFieldPosition() + 1;
    }


}
