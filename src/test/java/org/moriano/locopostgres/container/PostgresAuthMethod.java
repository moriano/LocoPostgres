package org.moriano.locopostgres.container;

/**
 * All the available Authentication methods available in postgres.
 *
 * Used to get containers that use each of these auth methods.
 */
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
