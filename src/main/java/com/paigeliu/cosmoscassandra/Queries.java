package com.paigeliu.cosmoscassandra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.List;
import java.util.LinkedList;

/**
 * Read test queries from properties file
 */
public class Queries {
    private static final Logger LOGGER = LoggerFactory.getLogger(Queries.class);
    private static String PROPERTY_FILE = "query.properties";
    private static Properties prop = null;

    private void loadProperties() throws IOException {
        InputStream input = getClass().getClassLoader().getResourceAsStream(PROPERTY_FILE);
        if (input == null) {
            LOGGER.error("unable to find {}", PROPERTY_FILE);
            return;
        }
        prop = new Properties();
        prop.load(input);
    }

    private String getProperty(String propertyName) throws IOException {
        if (prop == null) {
            loadProperties();
        }
        return prop.getProperty(propertyName);

    }

    public String CreateKeyspace() throws IOException {
        return getProperty("create_keyspace");
    }

    public String CreateTable() throws IOException {
        return getProperty("create_table");
    }

    public List<String> QueriesToRun() throws IOException {
        List<String> result = new LinkedList<>();
        String value;

        for(int i = 0; (value = getProperty("query." + i)) != null; i++) {
            result.add(value);
        }
        return result;
    }

    public String GetStressDataFile() throws IOException {
        return getProperty("stress_data_file");
    }
}
