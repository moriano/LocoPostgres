package org.moriano.locopostgres;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class LocoNetwork {
    private static final Logger log = LogManager.getLogger(LocoNetwork.class);
    private Socket socket;
    private InputStream inputStream;
    private OutputStream outputStream;

    public LocoNetwork(String host, int port) throws IOException  {

        socket = new Socket(host, port);
        inputStream = socket.getInputStream();
        outputStream = socket.getOutputStream();
    }

    public Packet readFromServer() throws SQLException {
        try {
            byte byteIdFromServer = inputStream.readNBytes(1)[0];
            byte[] packetRawSize = inputStream.readNBytes(4);
            int packetSize = ByteUtil.getInt32(packetRawSize) - 4;
            byte[] packetContents = inputStream.readNBytes(packetSize);
            byte[] fullServerPacket = ByteUtil.concat(new byte[]{byteIdFromServer}, packetRawSize, packetContents);
            Packet serverPacket = Packet.fromBytes(fullServerPacket);
            log.debug(serverPacket);
            return serverPacket;
        } catch(IOException e) {
            throw new SQLException(e);
        }
    }

    public boolean checkIfServerHasData() {
        try {
            return this.inputStream.available() > 0;
        } catch (IOException e) {
            log.error("No available bytes from inputstream", e);
            return false;
        }
    }

    public void sendPacketToServer(Packet packet) throws SQLException {
        log.debug(packet);
        try {
            outputStream.write(packet.getPacketContents());
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    public byte readOneByte() {
        try {
            return this.inputStream.readNBytes(1)[0];
        } catch (IOException e) {
            throw new RuntimeException("Could not read from server!", e);
        }
    }

    public byte[] readNBytes(int n) {
        try {
            return this.inputStream.readNBytes(n);
        } catch (IOException e) {
            throw new RuntimeException("Could not read from server!", e);
        }
    }

    public Packet readUntilPacketType(PacketType packetType) throws SQLException {
        PacketType backendPacketType = null;
        Packet serverPacket = null;
        do {
            serverPacket = this.readFromServer();
            backendPacketType = PacketType.backendPacketTypeFromByte(serverPacket.getPacketContents()[0]);
        } while (backendPacketType != packetType);
        return serverPacket;
    }

    public Packet readUntilPacketTypes(Set<PacketType> packetTypes) throws SQLException {
        PacketType backendPacketType = null;
        Packet serverPacket = null;
        do {
            serverPacket = this.readFromServer();
            backendPacketType = PacketType.backendPacketTypeFromByte(serverPacket.getPacketContents()[0]);
        } while (!packetTypes.contains(backendPacketType));
        return serverPacket;
    }

    public void waitUntilReadyForQuery() throws SQLException {
        PacketType backendPacketType = null;
        do {
            Packet serverPacket = this.readFromServer();
            backendPacketType = PacketType.backendPacketTypeFromByte(serverPacket.getPacketContents()[0]);
        } while (backendPacketType != PacketType.BACKEND_READY_FOR_QUERY);
    }

    public Packet waitUntilRowDescription() throws SQLException {
        PacketType backendPacketType = null;
        Packet serverPacket = null;
        do {
             serverPacket = this.readFromServer();
            backendPacketType = PacketType.backendPacketTypeFromByte(serverPacket.getPacketContents()[0]);
        } while (backendPacketType != PacketType.BACKEND_ROW_DESCRIPTION);
        return serverPacket;
    }

    public void close() {
        try {
            this.socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
