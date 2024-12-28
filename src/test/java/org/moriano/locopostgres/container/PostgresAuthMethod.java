package org.moriano.locopostgres.container;

public enum PostgresAuthMethod {

    MD5,
    TRUST,
    REJECT,
    SCRAM_SHA_256,
    PASSWORD,
    GSS,
    SSPI,
    IDENT,
    PEER,
    LDAP,
    RADIUS,
    CERT,
    PAM,
    BSD
}
