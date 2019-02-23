package com.paigeliu.cosmoscassandra;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Random;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Calendar;
import java.util.Date;

import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.OverloadedException;

import com.microsoft.azure.documentdb.*;
import com.microsoft.azure.documentdb.internal.HttpConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class gives implementations of create, delete table on Cassandra database
 * Insert & select data from the table
 */
public class CassandraTests {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraTests.class);
    private static Random random = new Random();
    private Session session;
    private Configurations config;
    private int numOfThreads = 64;
    private int maxAttemptsOnThrottle = 9;

    public CassandraTests(Session session, Configurations config) {
        this.session = session;
        this.config = config;
        try {this.numOfThreads = Integer.parseInt(config.getProperty("num_of_threads"));} catch (Exception e) {};
        try {this.maxAttemptsOnThrottle = Integer.parseInt(config.getProperty("max_attempts_on_throttle"));} catch (Exception e) {};
    }

    public void RunSimpleStatement(String sql) {
        SimpleStatement statement = new SimpleStatement(sql);
        long startTime = System.currentTimeMillis();
        ResultSet result = session.execute(statement);
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        LOGGER.info("{}: ExecutionTime, {}ms", sql, elapsedTime);
        for(ExecutionInfo ei: result.getAllExecutionInfo()) {
            Map<String, ByteBuffer> payloads = ei.getIncomingPayload();
            for (Map.Entry<String, ByteBuffer> payload: payloads.entrySet()) {
                String key = payload.getKey();
                ByteBuffer buf = payload.getValue();
                double requestCharge = buf.getDouble(buf.position());
                LOGGER.info("{}: {}, {}", sql, key, requestCharge);
            }
        }
    }

    public void CreateKeyspace(String query) {
        session.execute(query);
        LOGGER.info("{}: Created keyspace", query);
    }

    public void CreateTable(String query) {
        session.execute(query);
        LOGGER.info("{}: Created table");
    }

    public void DeleteTable(String query) {
        session.execute(query);
        LOGGER.info("{}: Deleted table");
    }

    public void RunLoadTest(String datafile) 
    {
        ExecutorService service = Executors.newScheduledThreadPool(numOfThreads);
        MappingManager manager = new MappingManager(session);
        Mapper<RecordToInsert> mapper_record_to_insert = manager.mapper(RecordToInsert.class);
        //read in the file and shuffle
        List<RecordToInsert> rows = ReadData(datafile); 
        Collections.shuffle(rows, random);
        int rowsTotal = rows.size();
        CountDownLatch latch = new CountDownLatch(rowsTotal);
        int rowsProcessed = 0;
        Date start = Calendar.getInstance().getTime();
        for (RecordToInsert row: rows)
        {
            service.submit(() -> 
            {
                int numOfAttempts = 0;
                Boolean shouldRetry = true;

                while (numOfAttempts < maxAttemptsOnThrottle && shouldRetry)
                {
                    try {
                        mapper_record_to_insert.save(row);
                        shouldRetry = false;
                    }
                    catch (NoHostAvailableException e)
                    {
                        shouldRetry = e.getMessage().contains("OverloadedException");
                        if (shouldRetry)
                        {
                            ++numOfAttempts;
                            LOGGER.warn("No host OverloadedException: {} times.", numOfAttempts);
                            ExponentialBackoff(numOfAttempts);
                        }
                        else
                        {
                            LOGGER.error("NoHostAvailableException: {}", e.getMessage());
                        }
                    }
                    catch (OverloadedException throttle) 
                    {
                        ++numOfAttempts;
                        LOGGER.warn("OverloadedException: {}th time.", numOfAttempts);
                        ExponentialBackoff(numOfAttempts);
                    }
                    catch (IllegalStateException e)
                    {
                        shouldRetry = HandleThrottle(e);
                        if (shouldRetry)
                        {
                            ++numOfAttempts;
                            LOGGER.error("Illegal State OverloadedException: {}", e.getMessage());
                            ExponentialBackoff(numOfAttempts);
                        }
                        else
                        {
                            LOGGER.error("IllegalStateException: {}", e.getMessage());
                        }
                    }
                    catch (Exception e)
                    {
                        LOGGER.error("Unknown Exception: {}", e.getMessage());
                        shouldRetry = false;
                    }
                }

                LOGGER.info("Latch countdown {}", latch.getCount());
                latch.countDown();
            });

            LOGGER.info("Processed {} rows", ++rowsProcessed);
        };

        try 
        {
            latch.await();
        }
        catch (InterruptedException e)
        {
            LOGGER.error ("Latch await exception: {}", e.getMessage());
        } 
        finally 
        {
            Date end = Calendar.getInstance().getTime();
            long durationInSec = (end.getTime() - start.getTime())/1000;
            LOGGER.info("Test took {} seconds.", durationInSec);
            service.shutdown();
        }
    }

    private Boolean HandleThrottle(IllegalStateException e)
    {
        Throwable exception = e;
        Boolean isThrottled = false;
        while (!(exception instanceof DocumentClientException) && exception != null)
        {
            exception = e.getCause();
        }

        if (exception != null)
        {
            DocumentClientException documentClientException = (DocumentClientException) exception;
            if (documentClientException.getStatusCode() == HttpConstants.StatusCodes.TOO_MANY_REQUESTS)
            {
                LOGGER.warn("Cosmos DB hit throttle with Http too many requests.");
                isThrottled = true;
            }
            else
            {
                LOGGER.error("HandleThrottle: {}", e.getMessage());
            }
        }

        return isThrottled;
    }

    private List<RecordToInsert> ReadData(String datafile) { 
        int count = 0;
        List<RecordToInsert> content = new ArrayList<RecordToInsert>();
        try(BufferedReader br = new BufferedReader(new FileReader(datafile))) {
            String line = "";
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");
                RecordToInsert record = new RecordToInsert();
                record.tid = new BigInteger(fields[0]);
                record.tidx = Double.parseDouble(fields[1]);
                record.tval = Float.parseFloat(fields[2]);
                content.add(record);
                ++count;
            }
            LOGGER.info("read in {} rows", count);
        } catch (FileNotFoundException e) {
            LOGGER.error("unable to find file {}", datafile);
        } catch (IOException e) {
            LOGGER.error("unable to read file {}", e.getMessage());
        }
        return content;
    }

    private static int PowerOfTwoWithCeiling(int power)
    {
        if (power <= 0) { return 1; }

        if (power > 30) { return PowerOfTwoWithCeiling(30); }

        int result = 1;
        for (int i = power; i > 0; i--) { result *= 2; }
        return result;
    }

    private void ExponentialBackoff(int numOfAttempts)
    {
        try {
            int waitTimeInMilliSeconds = PowerOfTwoWithCeiling(numOfAttempts) * 100;
            LOGGER.info("Exponential backoff: Retrying after Waiting for {} milliseconds", waitTimeInMilliSeconds);
            Thread.sleep(waitTimeInMilliSeconds);
        } catch (InterruptedException e) {
            LOGGER.error("Exponential backoff exception {}", e.getMessage());
        }
    }
}