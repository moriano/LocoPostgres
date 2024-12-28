package org.moriano.locopostgres.container;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.images.builder.Transferable;

public class PostgresTestContainer extends PostgreSQLContainer {

    private final PostgresAuthMethod postgresAuthMethod;

    public PostgresTestContainer(String dockerImageName, String database, String userName, String password, PostgresAuthMethod postgresAuthMethod) {
        super(dockerImageName);
        this.withUsername(userName);
        this.withPassword(password);
        this.withDatabaseName(database);
        this.postgresAuthMethod = postgresAuthMethod;
    }


    @Override
    protected void configure() {
        super.configure();

        if (postgresAuthMethod == PostgresAuthMethod.MD5) {
            addEnv("POSTGRES_INITDB_ARGS", "--auth=md5 --auth-host=md5 --auth-local=md5");
        } else if (postgresAuthMethod == PostgresAuthMethod.PASSWORD) {
            addEnv("POSTGRES_HOST_AUTH_METHOD", "password");
        } else if (postgresAuthMethod == PostgresAuthMethod.SCRAM_SHA_256) {
            addEnv("POSTGRES_HOST_AUTH_METHOD", "scram-sha-256");
        }
    }
}
