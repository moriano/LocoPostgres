package org.moriano.locopostgres;

/**
 * A simple enum that helps us know whether a packet is a FRONTEND (client) or BACKEND (server)
 */
public enum FrontendOrBackend {

    FRONTED,
    BACKEND
}
