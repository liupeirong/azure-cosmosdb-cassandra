package com.paigeliu.cosmoscassandra;

import com.datastax.driver.mapping.annotations.Table;
import java.math.BigInteger;

@Table(keyspace = "mytestks", name = "mytesttbl")
public class RecordToInsert {
    public BigInteger tid;
    public double tidx;
    public float tval;
}

