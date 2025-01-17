package org.moriano.locopostgres;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The Driver implementation of LocoPostgres
 */
public class LocoDriver implements Driver {
    private static final Logger log = LogManager.getLogger(LocoDriver.class);

    private static final String regex = "" +
            "(jdbc:loco:postgresql:\\w+)|(jdbc:loco:postgresql:\\/)|" +
            "(jdbc:loco:postgresql:\\/\\/\\w+\\/\\w+)|" +
            "(jdbc:loco:postgresql:\\/\\/\\w+\\/)|" +
            "(jdbc:loco:postgresql:\\/\\/\\w+:[0-9]+\\/\\w+)|" +
            "(jdbc:loco:postgresql:\\/\\/\\w+:[0-9]+\\/)\n";

    Pattern connectionPattern = Pattern.compile(regex);


    @Override
    public Connection connect(String s, Properties properties) throws SQLException {
        if (!this.acceptsURL(s)) {
            throw new SQLException("Cannot connect to " + s + " the URL seems invalid!");
        }

        Connection result = null;

        /*
        As per doc, username and password might come from either the url or as a property. If present in
        both, then the url value is the one used

        TODO moriano, read from the URL too
         */
        String user = properties.getProperty("user");
        String password = properties.getProperty("password");
        String database = properties.getProperty("database");

        // Jdbc url looks like jdbc:loco:postgresql://localhost:32829/testdb
        String host = s.split("//")[1].split(":")[0];
        int port = Integer.valueOf(s.split("//")[1].split(":")[1].split("/")[0]);
        if (user == null && password == null) {
            throw new SQLException("Cannot connect unless i have a username and a password");
        }

        Packet startupPacket = Packet.startupMessage(user, database);
        try {
            LocoNetwork locoNetwork = new LocoNetwork(host, port);


            locoNetwork.sendPacketToServer(startupPacket);
            Packet serverPacket = locoNetwork.readFromServer();
            byte byteIdFromServer = serverPacket.getPacketContents()[0];

            char idChar = ByteUtil.asChar(byteIdFromServer);
            if (idChar == 'R') {
                /*
                This is an authentication message, there are multiple authentication messages and they cannot just
                be determined by the id byte, extra analysis is required
                 */
                if (serverPacket.getPacketType() == PacketType.BACKEND_AUTHENTICATION_SASL) {
                    /*
                    We must authenticate using the SCRAM SHA 256 approach.

                    This is defined in the RFC-7677
                    https://datatracker.ietf.org/doc/html/rfc7677

                    Also, we need to read
                    https://datatracker.ietf.org/doc/html/rfc5802
                     */
                    byte[] backendAuthSASLBytes = serverPacket.getPacketContents();
                    /*
                    Remember, this packet structure is

                    id byte (p)
                    int32 with length of message
                    int32 with number 10 (indicates this is a sasl)
                    String with the name of the SASL auth mechanism
                     */
                    byte[] authenticationMechanism = Arrays.copyOfRange(backendAuthSASLBytes, 9, backendAuthSASLBytes.length-1);

                    Packet saslInitialRequest = Packet.saslInitialResponse(new String(authenticationMechanism));

                    locoNetwork.sendPacketToServer(saslInitialRequest);
                    locoNetwork.readFromServer();
                    throw new SQLException("LocoPostgres does not support SHA 256 authentication yet");
                }
                else if (serverPacket.getPacketType() == PacketType.BACKEND_AUTHENTICATION_CLEARTEXT_PASSWORD) {
                    /*
                     This is a plain text password authentication mechanism.

                     Pretty simply, we just need to send the password in plain text (yes, scary!) to the server
                     and await for a response
                     */
                    Packet passwordPacket = Packet.passwordMessage(password.getBytes());
                    locoNetwork.sendPacketToServer(passwordPacket);
                    serverPacket = locoNetwork.readFromServer();
                    if (serverPacket.getPacketType() == PacketType.BACKEND_AUTHENTICATION_OK) {
                        /*
                        At this point, we just need to read packets from the server until the server tells us it is
                        ready for query
                         */
                        BackendDataAndParameterStatus backendDataAndParameterStatus = processPacketsAfterAuthenticationOK(locoNetwork);

                        result = new LocoConnection(locoNetwork, backendDataAndParameterStatus.backendKeyData, backendDataAndParameterStatus.parameterStatuses);
                    } else {
                        throw new SQLException("Something crashed!, packet was " + serverPacket);
                    }
                }
                else if (serverPacket.getPacketType() == PacketType.BACKEND_AUTHENTICATION_MD5_PASSWORD) {
                    /*
                    We need to perform md5 authentication here. As per the official docs

                    The frontend must now send a Password Message containing the password (with user name)
                    en-crypted via MD5, then encrypted again using the 4-byte random salt specified in the
                    Authentication MD5Password message. If this is the correct password, the server responds
                    with an AuthenticationOk, otherwise it responds with an ErrorResponse.

                    The actual PasswordMessage can be computed in SQL as
                    concat('md5', md5(concat(md5(concat(password, user-name)), random-salt))).
                    (Keep in mind the md5() function returns its result as a hexstring.)
                     */
                    byte[] md5PacketContents = serverPacket.getPacketContents();

                    /*
                    Remember, the AutenticationMd5Password message has the structure
                    1 byte to id (R)
                    4 bytes for int32 for size, hardcoded to 12
                    4 bytes for int32 hardcoded to 5
                    4 bytes with the salt
                     */
                    byte[] randomSalt = Arrays.copyOfRange(md5PacketContents, 9, 13);

                    byte[] saltedMd5 = MD5Digest.encode(user.getBytes(), password.getBytes(), randomSalt);

                    Packet passwordMessage = Packet.passwordMessage(saltedMd5);
                    locoNetwork.sendPacketToServer(passwordMessage);
                    serverPacket = locoNetwork.readFromServer();
                    if (serverPacket.getPacketType() == PacketType.BACKEND_AUTHENTICATION_OK) {
                        /*
                        At this point, we just need to read packets from the server until the server tells us it is
                        ready for query
                         */
                        BackendDataAndParameterStatus backendDataAndParameterStatus = processPacketsAfterAuthenticationOK(locoNetwork);

                        result = new LocoConnection(locoNetwork, backendDataAndParameterStatus.backendKeyData, backendDataAndParameterStatus.parameterStatuses);
                    } else {
                        throw new SQLException("Something crashed!, packet was " + serverPacket);
                    }
                } else {
                    throw new SQLException("Cannot proceed, this driver only supports md5 and clear text passsword authentication");
                }
            }


        } catch (IOException e) {
            throw new SQLException("Cannot connect!", e);
        }

        return result;
    }

    /**
     * This method is to be called after we get an BACKEND_AUTHENTICATION_OK, it will take care of parsing all the
     * relevant packets that we receive until we get a READY_FOR_QUERY message.
     *
     * During that time, the server will send us packets that we need to store, for example the BACKEND_KEY_DATA which
     * is useful for cancelling queries as well as a number of configuration values
     */
    BackendDataAndParameterStatus processPacketsAfterAuthenticationOK(LocoNetwork locoNetwork) throws SQLException {
        Set<PacketType> relevantPacketTypes = Set.of(PacketType.BACKEND_KEY_DATA, PacketType.BACKEND_PARAMETER_STATUS);
        List<Packet> receivedPackets = new ArrayList<>();
        locoNetwork.readUntilPacketType(PacketType.BACKEND_READY_FOR_QUERY, relevantPacketTypes, receivedPackets);

        List<ParameterStatus> parameterStatuses = new ArrayList<>();
        BackendKeyData backendKeyData = null;
        for (Packet receivedPacket : receivedPackets) {
            if (receivedPacket.getPacketType() == PacketType.BACKEND_KEY_DATA) {
                backendKeyData = receivedPacket.getBackendKeyData();
            } else if (receivedPacket.getPacketType() == PacketType.BACKEND_PARAMETER_STATUS) {
                /*
                The parameter status packet is composed of
                1 byte for the packet type
                 */
                parameterStatuses.add(receivedPacket.getParameterStatus());
            }
        }

        return new BackendDataAndParameterStatus(backendKeyData, parameterStatuses);
    }


    /**
     * As per postgres docs, the suported urls are
     * <p>
     * jdbc:postgresql:database
     * jdbc:postgresql:/
     * jdbc:postgresql://host/database
     * jdbc:postgresql://host/
     * jdbc:postgresql://host:port/database
     * jdbc:postgresql://host:port/
     *
     * @param s
     * @return
     * @throws SQLException
     */
    @Override
    public boolean acceptsURL(String s) throws SQLException {
        Matcher matcher = connectionPattern.matcher(s);
        return matcher.find();
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String s, Properties properties) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    /**
     * A convenient class to bundle together the Backend data and parameter status.
     */
    private class BackendDataAndParameterStatus {
        final BackendKeyData backendKeyData;
        final List<ParameterStatus> parameterStatuses;

        public BackendDataAndParameterStatus(BackendKeyData backendKeyData, List<ParameterStatus> parameterStatuses) {
            this.backendKeyData = backendKeyData;
            this.parameterStatuses = parameterStatuses;
        }


    }
}
