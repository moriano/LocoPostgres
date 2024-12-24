package org.moriano.locopostgres;

import java.util.Arrays;

/**
 * Represents a Packet that is exchanged with the server.
 *
 * This comes from the Postgres protocol documentation, see
 * https://www.postgresql.org/docs/current/protocol-message-formats.html
 * for details.
 *
 * Each packet contains an Array of bytes and an field indicating the
 * type of the packet that it is.
 *
 *
 */
public class Packet {

    private PacketType packetType;
    private byte[] packetContents;


    public Packet(PacketType packetType, byte[] packetContents) {
        this.packetType = packetType;
        this.packetContents = packetContents;
    }

    public static Packet fromBytes(byte[] rawBytes) {
        char idChar = ByteUtil.asChar(rawBytes[0]);
        PacketType packetType = null;

        if (idChar == 'R') {
            /*
            Remember packet has
            1 byte to id it
            4 bytes to know the actual size
            4 bytes with an int that tells us which kind of authentication we need to use
             */
            int firstInt = ByteUtil.getInt32(new byte[]{rawBytes[5], rawBytes[6], rawBytes[7], rawBytes[8]});
            if (firstInt == 0) {
                packetType = PacketType.BACKEND_AUTHENTICATION_OK;
            } else if (firstInt == 2) {
                packetType = PacketType.BACKEND_AUTHENTICATION_KERBEROS_V5;
            } else if (firstInt == 3) {
                packetType = PacketType.BACKEND_AUTHENTICATION_CLEARTEXT_PASSWORD;
            } else if (firstInt == 5) {
                packetType = PacketType.BACKEND_AUTHENTICATION_MD5_PASSWORD;
            } else if (firstInt == 6) {
                packetType = PacketType.BACKEND_AUTHENTICATION_SCM_CREDENTIAL;
            } else if (firstInt == 7) {
                packetType = PacketType.BACKEND_AUTHENTICATION_GSS;
            } else if (firstInt == 8) {
                packetType = PacketType.BACKEND_AUTHENTICATION_GSS_CONTINUE;
            } else if (firstInt == 9) {
                packetType = PacketType.BACKEND_AUTHENTICATION_SSPI;
            } else if (firstInt == 10) {
                packetType = PacketType.BACKEND_AUTHENTICATION_SASL;
            } else if (firstInt == 11) {
                packetType = PacketType.BACKEND_AUTHENTICATION_SASL_CONTINUE;
            } else if (firstInt == 12) {
                packetType = PacketType.BACKEND_AUTHENTICATION_SASL_FINAL;
            }
        } else {
            packetType = PacketType.backendPacketTypeFromByte(rawBytes[0]);
        }
        if (packetType == null) {
            throw new RuntimeException("Could not identify packet " + ByteUtil.prettyPrint(rawBytes));
        }

        return new Packet(packetType, rawBytes);
    }

    /**
     * Builds a fully initialized startup packet
     *
     * @param user
     * @param database
     * @return
     */
    public static Packet startupMessage(String user, String database) {
        byte[] protocolVersion = ByteUtil.asBytes(196608);

        byte[] userParam = ByteUtil.asBytes("user");
        byte[] userValue = ByteUtil.asBytes(user);

        byte[] packetSoFar = ByteUtil.concat(protocolVersion,
                userParam, new byte[]{0x00}, userValue, new byte[]{0x00});

        if (database != null) {
            byte[] dbParam = ByteUtil.asBytes("database");
            byte[] dbValue = ByteUtil.asBytes(database);

            packetSoFar = ByteUtil.concat(packetSoFar,
                    dbParam, new byte[]{0x00}, dbValue, new byte[]{0x00});
        }

        packetSoFar = ByteUtil.concat(packetSoFar, new byte[]{0x00});

        int packetSize = packetSoFar.length + 4; //We need to include the first int32 as part of packet size
        byte[] finalPacket = ByteUtil.concat(ByteUtil.asBytes(packetSize), packetSoFar);

        return new Packet(PacketType.FRONTEND_STARTUP_MESSAGE, finalPacket);
    }

    /**
     * Prepares a password message to authenticate against the server
     *
     * @return
     */
    public static Packet passwordMessage(byte[] password) {
        byte[] byteId = ByteUtil.asBytes("p");
        byte[] packetSize = ByteUtil.asBytes(password.length + 5);

        byte[] result = ByteUtil.concat(byteId, packetSize, password, new byte[]{0x00});
        return new Packet(PacketType.FRONTEND_PASSWORD_MESSAGE, result);

    }

    /**
     * Prepares a simple protocol query packet using the input as the sql query.
     * @param sql
     * @return
     */
    public static Packet query(String sql) {
        byte[] sqlAsBytes = ByteUtil.concat(sql.getBytes(), new byte[]{0x00});
        byte[] sizeAsBytes = ByteUtil.asBytes(sqlAsBytes.length + 4);
        byte[] result = ByteUtil.concat(ByteUtil.asBytes("Q"), sizeAsBytes, sqlAsBytes);
        return new Packet(PacketType.FRONTEND_QUERY,
                result);

    }

    /**
     * Prepares a Parse message. This is useful in the extended protocol mode.
     * @param sql
     * @param statementName
     * @return
     */
    public static Packet parse(String sql, String statementName) {
        /*
        Structure

        Id byte is "P"
        int32 with message size
        String with name of the destination portal, empty string is the default portal
        String the query to be parsed
        Int16 number of parameter data types specified (can be zero)
        Then for each parameter
            int32 object id of the parameter type. Zero means unspecified.
        */
        byte[] sqlAsBytes = ByteUtil.concat(sql.getBytes(), new byte[]{0x00});
        int size = sqlAsBytes.length + 4 + 2;
        byte[] result;
        byte[] paramDataTypes = ByteUtil.asBytesInt16(0);
        if (statementName == null) {
            size += 1;
        } else {
            size = statementName.getBytes().length + 1;
        }
        byte[] sizeAsBytes = ByteUtil.asBytes(size);
        if (statementName !=null) {
            byte[] statementNameAsBytes = ByteUtil.concat(statementName.getBytes(), new byte[]{0x00});
            result = ByteUtil.concat(ByteUtil.asBytes("P"), sizeAsBytes, statementNameAsBytes, sqlAsBytes, paramDataTypes);
        } else {
            result = ByteUtil.concat(ByteUtil.asBytes("P"), sizeAsBytes, new byte[]{0x00}, sqlAsBytes, paramDataTypes);
        }

        return new Packet(PacketType.FRONTEND_PARSE, result);
    }

    /**
     * Creates a Bind packet, this is useful for the extended protocol mode.
     * @param paramPosition
     * @param value
     * @return
     */
    public static Packet bind(int paramPosition, byte[] value) {
        /*
        Structure

        Id byte is "B"
        int32 with length of packet
        String with destination portal (empty string selects the unnamed portal)
        String name of the source prepared statement (empty string selects the unnamed prepared statement)
        int16 the number of parameter format codes that follow. This can be zero to denote that all parameters use
                the default format (text). This is denoted as 'C' later
        int16[C] The parameter format codes, can be zero (text) or one (binary)
        int16 the number of parameters that follow (could be zero). This must match the number of parameters required
                byte the query
        Next, fhe following pair of fields appear for each parameter

        int32 lenght of the parameter value in bytes, can be zero. -1 means NULL
        Byte(n) The value of the parameter in the format indicated by the associated format code. n is the above length

        After the LAST parameter, the following fields must appear
        int16 the number of result-column format codes that follow (denoted R below). This can be zero to indicate
        that there are no result columns or that the result coliumns must use the default format (text)

        int16[R] the result column format codes. They can be zero (text) or one (binary)

         */
        String destinationPortal = null;

        return null;
    }

    /**
     * Generates an Execute packet. This is useful for the extended protocol mode.
     * @param portalName
     * @return
     */
    public static Packet execute(String portalName) {
        /*
        Structure

        Id byte is "E"
        int32 with size of packet
        String with name of the portal to execute (empty string is the default portal)
        int32 max number of rows to return, zero means no limit
         */
        int size = 4;
        byte[] portalNameInBytes;
        if (portalName == null) {
            size++;
            portalNameInBytes = new byte[]{0x00};
        } else {
            size += portalName.getBytes().length + 1;
            portalNameInBytes = ByteUtil.concat(portalName.getBytes(), new byte[]{0x00});
        }
        size += 4;


        byte[] result = ByteUtil.concat(ByteUtil.asBytes("E"), ByteUtil.asBytes(size),
                portalNameInBytes, ByteUtil.asBytes(0));
        return new Packet(PacketType.FRONTEND_EXECUTE, result);
    }

    /**
     * Creates a Startup message. These messages are used during the connection initialization.
     * @param user
     * @return
     */
    public static Packet startupMessage(String user) {
        return startupMessage(user, null);
    }

    /**
     * Creates a Terminate packet. This is the packet that is sent to the server to indicate that
     * the connection is about to be closed.
     * @return
     */
    public static Packet terminate() {
        byte[] byteId = ByteUtil.asBytes("X");
        byte[] packetSize = ByteUtil.asBytes(4);
        byte[] contents = ByteUtil.concat(byteId, packetSize);
        return new Packet(PacketType.FRONTEND_TERMINATE, contents);
    }

    public PacketType getPacketType() {
        return packetType;
    }


    public byte[] getPacketContents() {
        return packetContents;
    }

    /**
     * A human readable representation of the packet. This is very useful for human inspection in the logs.
     * Particularly useful for studying the protocol or debugging issues.
     * @return
     */
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("\n---------PACKET TYPE '"+this.packetType+"' STARTS------------\n\n");
        result.append(ByteUtil.prettyPrint(this.packetContents));
        result.append("\n---------PACKET TYPE '"+this.packetType+"' ENDS------------\n");
        return result.toString();

    }
}
