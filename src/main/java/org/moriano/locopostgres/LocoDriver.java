package org.moriano.locopostgres;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.Arrays;
import java.util.Properties;
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

        if (user == null && password == null) {
            throw new SQLException("Cannot connect unless i have a username and a password");
        }

        Packet startupPacket = Packet.startupMessage(user, database);
        try {
            LocoNetwork locoNetwork = new LocoNetwork("localhost", 5432);


            locoNetwork.sendPacketToServer(startupPacket);
            byte byteIdFromServer = locoNetwork.readOneByte();

            char idChar = ByteUtil.asChar(byteIdFromServer);
            if (idChar == 'R') {
                /*
                This is an authentication message, there are multiple authentication messages and they cannot just
                be determined by the id byte, extra analysis is required
                 */
                byte[] packetRawSize = locoNetwork.readNBytes(4);
                int packetSize = ByteUtil.getInt32(packetRawSize) - 4;
                byte[] packetContents = locoNetwork.readNBytes(packetSize);
                byte[] fullServerPacket = ByteUtil.concat(new byte[]{byteIdFromServer}, packetRawSize, packetContents);
                Packet serverPacket = Packet.fromBytes(fullServerPacket);

                if (serverPacket.getPacketType() == PacketType.BACKEND_AUTHENTICATION_MD5_PASSWORD) {
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
                        locoNetwork.waitUntilReadyForQuery();

                        result = new LocoConnection(locoNetwork);
                    } else {
                        throw new SQLException("Something crashed!, packet was " + serverPacket);
                    }
                } else {
                    throw new SQLException("Cannot proceed, this driver only supports md5 type authentication");
                }
            }


        } catch (IOException e) {
            throw new SQLException("Cannot connect!", e);
        }

        return result;
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
}
