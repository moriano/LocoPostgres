package org.moriano.locopostgres;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

public class ResultSetComparator {
    private static final Logger log = LogManager.getLogger(ResultSetComparator.class);

    public boolean compareResultSets(ResultSet postgres, ResultSet loco, MyFunction myFunction) throws SQLException {

        boolean moreLocoResults = loco.next();
        boolean morePostgresResults = postgres.next();

        boolean sameResult = true;
        do {
            Object postgresResult = myFunction.apply(postgres);
            Object locoResult = myFunction.apply(loco);

            boolean localSameResult = true;
            if (locoResult == null && postgresResult != null) {
                sameResult = false;
                localSameResult = false;
            } else if (locoResult != null && postgresResult == null) {
                sameResult = false;
                localSameResult = false;
            } else {
                sameResult = Objects.deepEquals(locoResult, postgresResult);
                localSameResult = sameResult;
            }

            if (!localSameResult) {
                log.warn("Watch out! \n The postgres result was \t'" + postgresResult + "'\n The loco result was \t '"
                        + locoResult + "'");
            }

            moreLocoResults = loco.next();
            morePostgresResults = postgres.next();
        } while (moreLocoResults || morePostgresResults);

        return sameResult;
    }

}
