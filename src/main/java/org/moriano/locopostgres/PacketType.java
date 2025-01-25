package org.moriano.locopostgres;


/**
 * A simple enum that keeps track of all the possible Packet types in the protocol.
 */
public enum PacketType {
    BACKEND_AUTHENTICATION_OK(FrontendOrBackend.BACKEND, 'R'),
    BACKEND_AUTHENTICATION_KERBEROS_V5(FrontendOrBackend.BACKEND, 'R'),
    BACKEND_AUTHENTICATION_CLEARTEXT_PASSWORD(FrontendOrBackend.BACKEND, 'R'),
    BACKEND_AUTHENTICATION_MD5_PASSWORD(FrontendOrBackend.BACKEND, 'R'),
    BACKEND_AUTHENTICATION_SCM_CREDENTIAL(FrontendOrBackend.BACKEND, 'R'),
    BACKEND_AUTHENTICATION_GSS(FrontendOrBackend.BACKEND, 'R'),
    BACKEND_AUTHENTICATION_GSS_CONTINUE(FrontendOrBackend.BACKEND, 'R'),
    BACKEND_AUTHENTICATION_SSPI(FrontendOrBackend.BACKEND, 'R'),
    BACKEND_AUTHENTICATION_SASL(FrontendOrBackend.BACKEND, 'R'),
    BACKEND_AUTHENTICATION_SASL_CONTINUE(FrontendOrBackend.BACKEND, 'R'),
    BACKEND_AUTHENTICATION_SASL_FINAL(FrontendOrBackend.BACKEND, 'R'),
    BACKEND_KEY_DATA(FrontendOrBackend.BACKEND, 'K'),
    FRONTEND_BIND(FrontendOrBackend.FRONTED, 'B'),
    BACKEND_BIND_COMPLETE(FrontendOrBackend.BACKEND, '2'),
    FRONTEND_CANCEL_REQUEST(FrontendOrBackend.FRONTED, null),
    FRONTEND_CLOSE(FrontendOrBackend.FRONTED, 'C'),
    BACKEND_CLOSE_COMPLETE(FrontendOrBackend.BACKEND, '3'),
    BACKEND_COMMAND_COMPLETE(FrontendOrBackend.BACKEND, 'C'),
    FRONTEND_COPY_DATA(FrontendOrBackend.FRONTED, 'd'),
    BACKEND_COPY_DATA(FrontendOrBackend.BACKEND, 'd'),
    FRONTEND_COPY_DONE(FrontendOrBackend.FRONTED, 'c'),
    BACKEND_COPY_DONE(FrontendOrBackend.BACKEND, 'c'),
    FRONTEND_COPY_FAIL(FrontendOrBackend.FRONTED, 'f'),
    BACKEND_COPY_IN_RESPONSE(FrontendOrBackend.BACKEND, 'G'),
    BACKEND_COPY_OUT_RESPONSE(FrontendOrBackend.BACKEND, 'H'),
    BACKEND_COPY_BOTH_RESPONSE(FrontendOrBackend.BACKEND, 'W'),
    BACKEND_DATA_ROW(FrontendOrBackend.BACKEND, 'D'),
    FRONTEND_DESCRIBE(FrontendOrBackend.FRONTED, 'D'),
    BACKEND_EMPTY_QUERY_RESPONSE(FrontendOrBackend.BACKEND, 'I'),
    BACKEND_ERROR_RESPONSE(FrontendOrBackend.BACKEND, 'E'),
    FRONTEND_EXECUTE(FrontendOrBackend.FRONTED, 'E'),
    FRONTEND_FLUSH(FrontendOrBackend.FRONTED, 'F'),
    FRONTEND_FUNCTION_CALL(FrontendOrBackend.FRONTED, 'F'),
    BACKEND_FUNCTION_CALL_RESPONSE(FrontendOrBackend.BACKEND, 'V'),
    FRONTEND_GSSENC_REQUEST(FrontendOrBackend.FRONTED, null),
    FRONTEND_GSS_RESPONSE(FrontendOrBackend.FRONTED, 'p'),
    BACKEND_NEGOTIATE_PROTOCOL_VERSION(FrontendOrBackend.BACKEND, 'v'),
    BACKEND_NO_DATA(FrontendOrBackend.BACKEND, 'n'),
    BACKEND_NOTICE_RESPONSE(FrontendOrBackend.BACKEND, 'N'),
    BACKEND_NOTIFICATION_RESPONSE(FrontendOrBackend.BACKEND, 'A'),
    BACKEND_PARAMETER_DESCRIPTION(FrontendOrBackend.BACKEND, 't'),
    BACKEND_PARAMETER_STATUS(FrontendOrBackend.BACKEND, 'S'),
    FRONTEND_PARSE(FrontendOrBackend.FRONTED, 'P'),
    BACKEND_PARSE_COMPLETE(FrontendOrBackend.BACKEND, '1'),
    FRONTEND_PASSWORD_MESSAGE(FrontendOrBackend.FRONTED, 'p'),
    BACKEND_PORTAL_SUSPENDED(FrontendOrBackend.BACKEND, 's'),
    FRONTEND_QUERY(FrontendOrBackend.FRONTED, 'Q'),
    BACKEND_READY_FOR_QUERY(FrontendOrBackend.BACKEND, 'Z'),
    BACKEND_ROW_DESCRIPTION(FrontendOrBackend.BACKEND, 'T'),
    FRONTEND_SASL_INITIAL_RESPONSE(FrontendOrBackend.FRONTED, 'p'),
    FRONTEND_SASL_RESPONSE(FrontendOrBackend.FRONTED, 'p'),
    FRONTEND_SSL_REQUEST(FrontendOrBackend.FRONTED, null),
    FRONTEND_STARTUP_MESSAGE(FrontendOrBackend.FRONTED, null),
    FRONTEND_SYNC(FrontendOrBackend.FRONTED, 'S'),
    FRONTEND_TERMINATE(FrontendOrBackend.FRONTED, 'X');


    /**
     * Indicates whether this is a frontend (client) or backend (server) packet type
     */
    private FrontendOrBackend frontendOrBackend;

    /**
     * The byte that identifies the packet type. Notice that some packets do NOT have
     * a startup byte
     */
    private Byte identificationByte;

    /**
     * The Character representation of the identification byte.
     */
    private Character identificationChar;

    PacketType(FrontendOrBackend frontendOrBackend, Character identificationChar) {
        this.frontendOrBackend = frontendOrBackend;

        this.identificationChar = identificationChar;
        this.identificationByte = identificationChar != null ? (byte)this.identificationChar.charValue() : null;
    }

    public FrontendOrBackend getFrontendOrBackend() {
        return frontendOrBackend;
    }

    public byte getIdentificationByte() {
        return identificationByte;
    }

    public Character getIdentificationChar() {
        return identificationChar;
    }

    public static PacketType backendPacketTypeFromByte(byte byteId) {
        for(PacketType packetType : values()) {
            if (packetType.frontendOrBackend == FrontendOrBackend.BACKEND &&
            byteId == packetType.identificationByte) {
                return packetType;
            }
        }
        return null;
    }

    public static PacketType frontendPacketTypeFromByte(byte byteId) {
        for(PacketType packetType : values()) {
            if (packetType.frontendOrBackend == FrontendOrBackend.FRONTED &&
                    byteId == packetType.identificationByte) {
                return packetType;
            }
        }
        return null;
    }
}
