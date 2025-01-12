package org.moriano.locopostgres;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

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
     * Prepares a simple protocol query packet that contains multiple statements.
     *
     * Unlike simple query messages, each of the statemens must be separated by ';'
     *
     * Remember that each statement must be terminated by byte 0x00
     * @param sqls
     * @return
     */
    public static Packet query(List<String> sqls) {
        int totalSize = 4; // The total size must include an int32
        byte[] sqlsAsBytes = new byte[]{};
        for (String sql : sqls) {
            totalSize += sql.length();

            if (sql.charAt(sql.length()-1) != ';') {
                totalSize += 1;
                sqlsAsBytes = ByteUtil.concat(sqlsAsBytes, sql.getBytes(Charset.defaultCharset()), new byte[]{";".getBytes()[0]});
            } else {
                sqlsAsBytes = ByteUtil.concat(sqlsAsBytes, sql.getBytes(Charset.defaultCharset()));
            }
        }
        sqlsAsBytes = ByteUtil.concat(sqlsAsBytes, new byte[]{0x00});
        totalSize += 1;


        byte[] result = ByteUtil.concat(ByteUtil.asBytes("Q"), ByteUtil.asBytes(totalSize), sqlsAsBytes);
        return new Packet(PacketType.FRONTEND_QUERY, result);

    }

    /**
     * Used as part of the SASL authentication process. This packet is what the client must sent to
     * the server after the server indicates that it uspports SASL authentication.
     *
     * This packet informs the server about which specific SASL authentication mechanism it wants to
     * use.
     * @return
     */
    public static Packet saslInitialResponse(String saslAuthMechanism) {
        /*
        Structure

        Id byte is 'p'
        int32 with the message size
        String with the name of the SASL auth mechanism to use
        int32 length of the SASL mechanism specific "Initial client response" that follows or -1 if there is no
                initial response
        Byte(n) SASL mechanism specific "Initial response"
         */

        /*
        Here is a valid packet sent from the postgres driver

        0000   70 00 00 00 37 53 43 52 41 4d 2d 53 48 41 2d 32   p...7SCRAM-SHA-2
        0010   35 36 00 00 00 00 21 6e 2c 2c 6e 3d 2a 2c 72 3d   56....!n,,n=*,r=
        0020   34 2a 65 78 4e 4f 24 30 3a 39 77 25 6d 37 6c 61   4*exNO$0:9w%m7la
        0030   4c 31 50 49 2e 5d 28 49                           L1PI.](I

        Server response

        0000   52 00 00 00 5c 00 00 00 0b 72 3d 34 2a 65 78 4e   R...\....r=4*exN
        0010   4f 24 30 3a 39 77 25 6d 37 6c 61 4c 31 50 49 2e   O$0:9w%m7laL1PI.
        0020   5d 28 49 79 71 49 76 2b 74 49 68 37 4f 74 75 75   ](IyqIv+tIh7Otuu
        0030   71 4b 78 45 66 68 73 50 4d 42 33 2c 73 3d 76 76   qKxEfhsPMB3,s=vv
        0040   4b 52 32 52 5a 51 4f 48 30 72 53 6c 63 6b 53 2f   KR2RZQOH0rSlckS/
        0050   74 65 56 51 3d 3d 2c 69 3d 34 30 39 36            teVQ==,i=4096


        Client request

        0000   52 00 00 00 5c 00 00 00 0b 72 3d 34 2a 65 78 4e   R...\....r=4*exN
        0010   4f 24 30 3a 39 77 25 6d 37 6c 61 4c 31 50 49 2e   O$0:9w%m7laL1PI.
        0020   5d 28 49 79 71 49 76 2b 74 49 68 37 4f 74 75 75   ](IyqIv+tIh7Otuu
        0030   71 4b 78 45 66 68 73 50 4d 42 33 2c 73 3d 76 76   qKxEfhsPMB3,s=vv
        0040   4b 52 32 52 5a 51 4f 48 30 72 53 6c 63 6b 53 2f   KR2RZQOH0rSlckS/
        0050   74 65 56 51 3d 3d 2c 69 3d 34 30 39 36            teVQ==,i=4096

        Server response (Auth completed)

        0000   52 00 00 00 36 00 00 00 0c 76 3d 50 45 71 36 53   R...6....v=PEq6S
        0010   6b 6a 38 49 4d 66 48 33 62 65 47 46 32 66 57 67   kj8IMfH3beGF2fWg
        0020   56 57 56 35 6c 5a 4b 47 47 41 7a 4c 69 5a 44 51   VWV5lZKGGAzLiZDQ
        0030   51 6d 6b 75 4b 6f 3d                              QmkuKo=


        And this is the one i am building

       +--------------------------------------------------+----------------------------------+------------------+
       | 00 01 02 03 04 05 06 07 08 09 0A 0B C0 D0 E0 0F  | 0 1 2 3 4 5 6 7 8 9 A B C D E F  |      Ascii       |
+------+--------------------------------------------------+----------------------------------+------------------+
| 0000 | 70 00 00 00 18 53 43 52 41 4D 2D 53 48 41 2D 32  | p . . . . S C R A M . S H A . 2  | p....SCRAM.SHA.2 |
| 0001 | 35 36 00 00 FF FF FF FF 00                       | 5 6 . . . . . . .                | 56.......        |
+------+--------------------------------------------------+----------------------------------+------------------+

         */




        byte[] idByte = ByteUtil.asBytes("p");

        byte[] mechanismAsBytes = ByteUtil.concat(saslAuthMechanism.getBytes(), new byte[]{0x00});
        byte[] initialResponse = "n,,n=*,r=random".getBytes();

        byte[] sizeAsBytes = ByteUtil.asBytes(mechanismAsBytes.length + 4 + 4 + initialResponse.length);

        byte[] result = ByteUtil.concat(idByte, sizeAsBytes, mechanismAsBytes, ByteUtil.asBytes(initialResponse.length), initialResponse);

        return new Packet(PacketType.FRONTEND_SASL_INITIAL_RESPONSE, result);

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
