package com.paigeliu.cosmoscassandra;

import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
    private static final Logger LOGGER = LoggerFactory.getLogger(App.class);

    public static void main(String[] s) {

        CassandraUtils utils = new CassandraUtils();
        Session cassandraSession = utils.getSession();
        Configurations config = utils.getConfig();

        try {
            CassandraTests tests = new CassandraTests(cassandraSession, config);
            Queries queries = new Queries();

            if (s.length > 0 && s[0].equals("--load")) 
            {
                tests.RunLoadTest(queries.GetStressDataFile());
            }
            else 
            {
                tests.CreateKeyspace(queries.CreateKeyspace());
                tests.CreateTable(queries.CreateTable());
                for (String query: queries.QueriesToRun())
                {
                    tests.RunSimpleStatement(query);
                }
            }
        } 
        catch (Exception ex)
        {
            LOGGER.error(ex.getMessage());
        }
        finally {
            utils.close();
        }
    }
}
