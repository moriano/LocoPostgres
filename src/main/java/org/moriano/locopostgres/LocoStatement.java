package org.moriano.locopostgres;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.sql.*;
import java.util.List;
import java.util.Set;

public class LocoStatement implements Statement {

    private LocoNetwork locoNetwork;
    public LocoStatement(LocoNetwork locoNetwork) {
        this.locoNetwork = locoNetwork;
    }
    private ResultSet locoResultSet;

    @Override
    public long getLargeUpdateCount() throws SQLException {
        return 0;
    }

    @Override
    public ResultSet executeQuery(String s) throws SQLException {
        Packet packet = Packet.query(s);
        this.locoNetwork.sendPacketToServer(packet);
        Packet serverPacket = this.locoNetwork.readUntilPacketTypes(Set.of(PacketType.BACKEND_ROW_DESCRIPTION, PacketType.BACKEND_READY_FOR_QUERY));
        if (serverPacket.getPacketType() == PacketType.BACKEND_ROW_DESCRIPTION) {
            Packet rowDescription = serverPacket;
            LocoRowDescription locoRowDescription = new LocoRowDescription(rowDescription);
            this.locoResultSet = new LocoResultSet(this.locoNetwork, locoRowDescription);
            return locoResultSet;
        } else {
            return LocoResultSet.emptyResultSet();
        }

    }

    @Override
    public int executeUpdate(String s) throws SQLException {
        return 0;
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int i) throws SQLException {

    }

    @Override
    public int getMaxRows() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxRows(int i) throws SQLException {

    }

    @Override
    public void setEscapeProcessing(boolean b) throws SQLException {

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int i) throws SQLException {

    }

    @Override
    public void cancel() throws SQLException {

    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String s) throws SQLException {

    }

    /**
     *
     * @param s
     * @return true if the first result is a ResultSet object; false if it is an update count or there are no results
     * @throws SQLException
     */
    @Override
    public boolean execute(String s) throws SQLException {
        Packet packet = Packet.query(s);
        this.locoNetwork.sendPacketToServer(packet);
        Packet serverPacket = this.locoNetwork.readUntilPacketTypes(Set.of(PacketType.BACKEND_ROW_DESCRIPTION, PacketType.BACKEND_READY_FOR_QUERY));
        if (serverPacket.getPacketType() == PacketType.BACKEND_ROW_DESCRIPTION) {
            boolean result = true;
            this.locoNetwork.readUntilPacketType(PacketType.BACKEND_READY_FOR_QUERY);
            return result;
        } else {
            return false;
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        if (this.locoResultSet == null) {
            throw new SQLException("No resultset is available!");
        }
        return this.locoResultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
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
    public int getResultSetConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return 0;
    }

    @Override
    public void addBatch(String s) throws SQLException {

    }

    @Override
    public void clearBatch() throws SQLException {

    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];
    }

    @Override
    public Connection getConnection() throws SQLException {
        return null;
    }

    @Override
    public boolean getMoreResults(int i) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(String s, int i) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String s, int[] ints) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String s, String[] strings) throws SQLException {
        return 0;
    }

    @Override
    public boolean execute(String s, int i) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String s, int[] ints) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String s, String[] strings) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean b) throws SQLException {

    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> aClass) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        return false;
    }
}
