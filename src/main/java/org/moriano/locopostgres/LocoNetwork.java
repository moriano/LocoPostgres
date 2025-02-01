package org.moriano.locopostgres;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

/**
 * This is THE object that controls sending and receiving traffic to and from the server.
 *
 * Postgres protocol is defined as arrays of bytes that are sent and received to and from the server.
 *
 * The implementation here is not done in an optimal way, instead it is done in a very simple human
 * readable way. Generally speaking every packet of the protocol is composed by an Id byte followed by
 * 4 bytes representing an int32 that specifies the length of the packet. This class uses that information
 * to understand how many more bytes we have to read from the server.
 *
 * This class is ready to log every packet that is sent and received to and from the server. The rationale to
 * log those packets is to facilitate the study of the protocol.
 */
public class LocoNetwork {
    private static final Logger log = LogManager.getLogger(LocoNetwork.class);
    private Socket socket;
    private InputStream inputStream;
    private OutputStream outputStream;
    private final String host;
    private final int port;

    public LocoNetwork(String host, int port) throws IOException  {

        socket = new Socket(host, port);
        inputStream = socket.getInputStream();
        outputStream = socket.getOutputStream();
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    /**
     * Closes all network resources. After calling this method, this object can no longer be used
     */
    public void cleanupResources() {
        try {
            if (this.inputStream != null) {
                this.inputStream.close();
            }
        } catch (Exception e) {
            log.error("Could not close the inputstream!", e);
        }

        try {
            if (this.outputStream != null) {
                this.outputStream.close();
            }
        } catch (Exception e) {
            log.error("Could not close the outputstream!", e);
        }

        try {
            if (this.socket != null) {
                this.socket.close();
            }
        } catch (Exception e) {
            log.error("Could not close the socket!", e);
        }
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

    /**
     * Keeps reading from the server until a packer of type #packetType is received
     * @param packetType
     * @return
     * @throws SQLException
     */
    public Packet readUntilPacketType(PacketType packetType) throws SQLException {
        PacketType backendPacketType = null;
        Packet serverPacket = null;
        do {
            serverPacket = this.readFromServer();
            backendPacketType = PacketType.backendPacketTypeFromByte(serverPacket.getPacketContents()[0]);
        } while (backendPacketType != packetType);
        return serverPacket;
    }

    /**
     * Reads until the server sends a packet of type packetType, the parameter receivedPackets will be modified
     * and will contain each of the packets received until then, unless they are of type packetTypesToIgnore
     *
     * Be careful when using this method, as it will sore all matching received packets in memory until a packetType
     * is received. Specifically if you use this method while processing a query result that could potentially store
     * many (millions?) of results, you want to make sure that you send a sensible filter of packets to ignore.
     * @param packetType
     * @ packetTypesToIgnore
     * @param receivedPackets
     * @return
     * @throws SQLException
     */
    public Packet readUntilPacketType(PacketType packetType, Set<PacketType> packetTypesToIgnore, List<Packet> receivedPackets) throws SQLException {
        PacketType backendPacketType = null;
        Packet serverPacket = null;
        do {
            serverPacket = this.readFromServer();
            backendPacketType = PacketType.backendPacketTypeFromByte(serverPacket.getPacketContents()[0]);
            if (packetTypesToIgnore.contains(backendPacketType)) {
                receivedPackets.add(serverPacket);
            }
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
