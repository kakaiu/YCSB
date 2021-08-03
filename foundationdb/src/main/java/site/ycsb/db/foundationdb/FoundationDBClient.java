/**
 * Copyright (c) 2012 - 2021 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db.foundationdb;

import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.tuple.Tuple;

import site.ycsb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import java.util.*;

/**
 * FoundationDB client for YCSB framework.
 */

public class FoundationDBClient extends DB {
  private static FDB fdb;
  private static Database[] dbs;
  private String dbName;
  private int batchSize;
  private int[] batchCounts;
  private boolean setPriorityBatch;
  private boolean setTransactionTrace;
  private int transactionTraceFraction;
  private String debugTransactionIdentifier;
  private static final String API_VERSION                = "foundationdb.apiversion";
  private static final String API_VERSION_DEFAULT        = "630";
  private static final String CLUSTER_FILE               = "foundationdb.clusterfile";
  private static final String CLUSTER_FILE_DEFAULT       = "./fdb.cluster";
  private static final String DB_NAME                    = "foundationdb.dbname";
  private static final String DB_NAME_DEFAULT            = "DB";
  private static final String DB_BATCH_SIZE_DEFAULT      = "0";
  private static final String DB_BATCH_SIZE              = "foundationdb.batchsize";
  private static final String DATACENTER_ID_DEFAULT      = "";
  private static final String DATACENTER_ID              = "foundationdb.datacenterid";
  private static final String SET_PRIORITY_BATCH         = "foundationdb.setprioritybatch";
  private static final String SET_PRIORITY_BATCH_DEFAULT = "";
  private static final String CLIENT_THREADS_PER_VERSION = "foundationdb.clientthreadsperversion";
  private static final String CLIENT_THREADS_PER_VERSION_DEFAULT = "0";
  private static final String EXTERNAL_CLIENT_DIRECTORY  = "foundationdb.externalclientdirectory";
  private static final String EXTERNAL_CLIENT_DIRECTORY_DEFAULT = ".";
  private static final String TRACE_DIRECTORY            = "foundationdb.tracedirectory";
  private static final String TRACE_DIRECTORY_DEFAULT    = "";
  private static final String SET_TRANSACTION_TRACE      = "foundationdb.settransactiontrace";
  private static final String SET_TRANSACTION_TRACE_DEFAULT = "";
  private static final String TRANSACTION_TRACE_FRACTION = "foundationdb.transactiontracefraction";
  private static final String TRANSACTION_TRACE_FRACTION_DEFAULT = "10000";
  private static final String DEBUG_TRANSACTION_IDENTIFIER = "foundationdb.debugtransactionidentifier";
  private static final String DEBUG_TRANSACTION_IDENTIFIER_DEFAULT = "test";
  private static final String TRACE_FORMAT               = "foundationdb.traceformat";
  private static final String TRACE_FORMAT_DEFAULT       = "json";

  private Vector<String>[] batchKeys;
  private Vector<Map<String, ByteIterator>>[] batchValues;

  private static Logger logger = LoggerFactory.getLogger(FoundationDBClient.class);

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    // initialize FoundationDB driver
    final Properties props = getProperties();
    String apiVersion = props.getProperty(API_VERSION, API_VERSION_DEFAULT);
    String clusterFileString = props.getProperty(CLUSTER_FILE, CLUSTER_FILE_DEFAULT);
    String[] clusterFiles = clusterFileString.split(",");
    logger.error("clusterFileString: {}", clusterFileString);
    logger.error("cluster file length: {}", clusterFiles.length);
    String dbBatchSize = props.getProperty(DB_BATCH_SIZE, DB_BATCH_SIZE_DEFAULT);
    dbName = props.getProperty(DB_NAME, DB_NAME_DEFAULT);
    String datacenterId = props.getProperty(DATACENTER_ID, DATACENTER_ID_DEFAULT);
    setPriorityBatch = props.getProperty(SET_PRIORITY_BATCH, SET_PRIORITY_BATCH_DEFAULT).equals("true");
    int clientThreadsPerVersion
        = Integer.parseInt(props.getProperty(CLIENT_THREADS_PER_VERSION, CLIENT_THREADS_PER_VERSION_DEFAULT));
    String externalClientDirectory
        = props.getProperty(EXTERNAL_CLIENT_DIRECTORY, EXTERNAL_CLIENT_DIRECTORY_DEFAULT);
    String traceDirectory = props.getProperty(TRACE_DIRECTORY, TRACE_DIRECTORY_DEFAULT);
    String traceFormat = props.getProperty(TRACE_FORMAT, TRACE_FORMAT_DEFAULT);
    logger.info("API Version: {}", apiVersion);
    logger.info("Cluster Files: {}\n", clusterFileString);
    setTransactionTrace = props.getProperty(SET_TRANSACTION_TRACE, SET_TRANSACTION_TRACE_DEFAULT).equals("true");
    transactionTraceFraction
        = Integer.parseInt(props.getProperty(TRANSACTION_TRACE_FRACTION, TRANSACTION_TRACE_FRACTION_DEFAULT));
    debugTransactionIdentifier = props.getProperty(DEBUG_TRANSACTION_IDENTIFIER, DEBUG_TRANSACTION_IDENTIFIER_DEFAULT);

    try {
      synchronized(FoundationDBClient.class) {
        if (fdb == null) {
          // Must only be called once per process.
          fdb = FDB.selectAPIVersion(Integer.parseInt(apiVersion.trim()));

          if (clientThreadsPerVersion != 0) {
            logger.info("Threads per version: {}", clientThreadsPerVersion);
            fdb.options().setClientThreadsPerVersion(clientThreadsPerVersion);
            if (externalClientDirectory == EXTERNAL_CLIENT_DIRECTORY_DEFAULT) { 
              throw new IllegalArgumentException(EXTERNAL_CLIENT_DIRECTORY + " must be set because " 
                + CLIENT_THREADS_PER_VERSION + " is set.");
            }
            fdb.options().setExternalClientDirectory(externalClientDirectory);
            if (traceDirectory != TRACE_DIRECTORY_DEFAULT) {
              logger.info("FDB trace directory: {}", traceDirectory);
              fdb.options().setTraceEnable(traceDirectory);
              fdb.options().setTraceFormat(traceFormat);
            }
          }
        }
      }

      dbs = new Database[clusterFiles.length];
      for (int i = 0; i < clusterFiles.length; i++) {
        dbs[i] = fdb.open(clusterFiles[i]);
        if (datacenterId != "") {
          logger.info("Datacenter ID: {}", datacenterId);
          dbs[i].options().setDatacenterId(datacenterId);
        }
      }

      batchSize = Integer.parseInt(dbBatchSize);
      batchCounts = new int[dbs.length];
      batchKeys = new Vector[dbs.length];
      batchValues = new Vector[dbs.length];
      for (int i = 0; i < dbs.length; i++) {
        batchKeys[i] = new Vector<String>(batchSize+1);
        batchValues[i] = new Vector<Map<String, ByteIterator>>(batchSize+1);
      }
    } catch (FDBException e) {
      logger.error(MessageFormatter.format("Error in database operation: {}", "init").getMessage(), e);
      throw new DBException(e);
    } catch (NumberFormatException e) {
      logger.error(MessageFormatter.format("Invalid value for apiversion property: {}", apiVersion).getMessage(), e);
      throw new DBException(e);
    }
  }

  @Override
  public void cleanup() throws DBException {
    for (int i = 0; i < dbs.length; i++) {
      if (batchCounts[i] > 0) {
        batchInsert(i);
        batchCounts[i] = 0;
      }
    }
    try {
      for (int i = 0; i < dbs.length; i++) {
        dbs[i].close();
      }
    } catch (FDBException e) {
      logger.error(MessageFormatter.format("Error in database operation: {}", "cleanup").getMessage(), e);
      throw new DBException(e);
    }
  }

  private static String getRowKey(String db, String table, String key) {
    //return key + ":" + table + ":" + db;
    return db + ":" + table + ":" + key;
  }

  private static String getEndRowKey(String table) {
    return table + ";";
  }

  private Status convTupleToMap(Tuple tuple, Set<String> fields, Map<String, ByteIterator> result) {
    for (int i = 0; i < tuple.size(); i++) {
      Tuple v = tuple.getNestedTuple(i);
      String field = v.getString(0);
      String value = v.getString(1);
      //System.err.println(field + " : " + value);
      result.put(field, new StringByteIterator(value));
    }
    if (fields != null) {
      for (String field : fields) {
        if (result.get(field) == null) {
          logger.debug("field not fount: {}", field);
          return Status.NOT_FOUND;
        }
      }
    }
    return Status.OK;
  }

  private void batchInsert(int dbIndex) {
    try {
      dbs[dbIndex].run(tr -> {
          if (setTransactionTrace && Math.random()<1.0/transactionTraceFraction) {
            tr.options().setDebugTransactionIdentifier(debugTransactionIdentifier);
            tr.options().setLogTransaction();
            tr.options().setServerRequestTracing();
          }
          for (int i = 0; i < batchCounts[dbIndex]; ++i) {
            String key = batchKeys[dbIndex].get(i);
            Tuple t = new Tuple();
            Map<String, ByteIterator> valueBytes = batchValues[dbIndex].get(i);
            for (Map.Entry<String, String> entry : StringByteIterator.getStringMap(valueBytes).entrySet()) {
              Tuple v = new Tuple();
              v = v.add(entry.getKey());
              v = v.add(entry.getValue());
              t = t.add(v);
            }
            tr.set(Tuple.from(key).pack(), t.pack());
          }
          return null;
        });
    } catch (FDBException e) {
      for (int i = 0; i < batchCounts[dbIndex]; ++i) {
        String key = batchKeys[dbIndex].get(i);
        logger.error(MessageFormatter.format("Error batch inserting key {}", key).getMessage(), e);
      }
      e.printStackTrace();
    } catch (Throwable e) {
      for (int i = 0; i < batchCounts[dbIndex]; ++i) {
        String key = batchKeys[dbIndex].get(i);
        logger.error(MessageFormatter.format("Error batch inserting key {}", key).getMessage(), e);
      }
      e.printStackTrace();
    } finally {
      batchKeys[dbIndex].clear();
      batchValues[dbIndex].clear();
    }
  }

  int dbFromKey(String key) {
    logger.error("dbs length {}", dbs.length);
    logger.error("tmp index {}", key.hashCode());
    logger.error("index {}", (key.hashCode() & Integer.MAX_VALUE) % dbs.length);
    //return (key.hashCode() & Integer.MAX_VALUE) % dbs.length;
    return 0;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    int dbIndex = dbFromKey(key);
    String rowKey = getRowKey(dbName, table, key);
    logger.debug("insert key = {}", rowKey);
    try {
      batchKeys[dbIndex].addElement(rowKey);
      batchValues[dbIndex].addElement(new HashMap<String, ByteIterator>(values));
      batchCounts[dbIndex]++;
      if (batchSize == 0 || batchSize == batchCounts[dbIndex]) {
        batchInsert(dbIndex);
        batchCounts[dbIndex] = 0;
      }
      return Status.OK;
    } catch (Throwable e) {
      logger.error(MessageFormatter.format("Error inserting key: {}", rowKey).getMessage(), e);
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    int dbIndex = dbFromKey(key);
    String rowKey = getRowKey(dbName, table, key);
    logger.debug("delete key = {}", rowKey);
    try {
      dbs[dbIndex].run(tr -> {
          if (setPriorityBatch) {
            tr.options().setPriorityBatch();
          }
          if (setTransactionTrace && Math.random()<1.0/transactionTraceFraction) {
            tr.options().setDebugTransactionIdentifier(debugTransactionIdentifier);
            tr.options().setLogTransaction();
            tr.options().setServerRequestTracing();
          }
          tr.clear(Tuple.from(rowKey).pack());
          return null;
        });
      return Status.OK;
    } catch (FDBException e) {
      logger.error(MessageFormatter.format("Error deleting key: {}", rowKey).getMessage(), e);
      e.printStackTrace();
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error deleting key: {}", rowKey).getMessage(), e);
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    int dbIndex = dbFromKey(key);
    String rowKey = getRowKey(dbName, table, key);
    logger.debug("read key = {}", rowKey);
    try {
      byte[] row = dbs[dbIndex].run(tr -> {
          if (setPriorityBatch) {
            tr.options().setPriorityBatch();
          }
          if (setTransactionTrace && Math.random()<1.0/transactionTraceFraction) {
            tr.options().setDebugTransactionIdentifier(debugTransactionIdentifier);
            tr.options().setLogTransaction();
            tr.options().setServerRequestTracing();
          }
          byte[] r = tr.get(Tuple.from(rowKey).pack()).join();
          return r;
        });
      Tuple t = Tuple.fromBytes(row);
      if (t.size() == 0) {
        logger.debug("key not fount: {}", rowKey);
        return Status.NOT_FOUND;
      }
      return convTupleToMap(t, fields, result);
    } catch (FDBException e) {
      logger.error(MessageFormatter.format("Error reading key: {}", rowKey).getMessage(), e);
      e.printStackTrace();
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error reading key: {}", rowKey).getMessage(), e);
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    int dbIndex = dbFromKey(key);
    String rowKey = getRowKey(dbName, table, key);
    logger.debug("update key = {}", rowKey);
    try {
      Status s = dbs[dbIndex].run(tr -> {
          if (setPriorityBatch) {
            tr.options().setPriorityBatch();
          }
          if (setTransactionTrace && Math.random()<1.0/transactionTraceFraction) {
            tr.options().setDebugTransactionIdentifier(debugTransactionIdentifier);
            tr.options().setLogTransaction();
            tr.options().setServerRequestTracing();
          }
          byte[] row = tr.get(Tuple.from(rowKey).pack()).join();
          Tuple o = Tuple.fromBytes(row);
          if (o.size() == 0) {
            logger.debug("key not fount: {}", rowKey);
            return Status.NOT_FOUND;
          }
          HashMap<String, ByteIterator> result = new HashMap<>();
          if (convTupleToMap(o, null, result) != Status.OK) {
            return Status.ERROR;
          }
          for (String k : values.keySet()) {
            if (result.containsKey(k)) {
              result.put(k, values.get(k));
            } else {
              logger.debug("field not fount: {}", k);
              return Status.NOT_FOUND;
            }
          }
          Tuple t = new Tuple();
          for (Map.Entry<String, String> entry : StringByteIterator.getStringMap(result).entrySet()) {
            Tuple v = new Tuple();
            v = v.add(entry.getKey());
            v = v.add(entry.getValue());
            t = t.add(v);
          }
          tr.set(Tuple.from(rowKey).pack(), t.pack());
          return Status.OK;
        });
      return s;
    } catch (FDBException e) {
      logger.error(MessageFormatter.format("Error updating key: {}", rowKey).getMessage(), e);
      e.printStackTrace();
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error updating key: {}", rowKey).getMessage(), e);
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    // TODO: Add flag to broadcast scans to all clusters instead of scanning just one cluster?
    int dbIndex = dbFromKey(startkey);
    String startRowKey = getRowKey(dbName, table, startkey);
    String endRowKey = getEndRowKey(table);
    logger.debug("scan key from {} to {} limit {} ", startkey, endRowKey, recordcount);
    try (Transaction tr = dbs[dbIndex].createTransaction()) {
      tr.options().setReadYourWritesDisable();
      if (setPriorityBatch) {
        tr.options().setPriorityBatch();
      }
      if (setTransactionTrace && Math.random()<1.0/transactionTraceFraction) {
        tr.options().setDebugTransactionIdentifier(debugTransactionIdentifier);
        tr.options().setLogTransaction();
        tr.options().setServerRequestTracing();
      }
      AsyncIterable<KeyValue> entryList = tr.getRange(Tuple.from(startRowKey).pack(), Tuple.from(endRowKey).pack(),
          recordcount > 0 ? recordcount : 0);
      List<KeyValue> entries = entryList.asList().join();
      for (int i = 0; i < entries.size(); ++i) {
        final HashMap<String, ByteIterator> map = new HashMap<>();
        Tuple value = Tuple.fromBytes(entries.get(i).getValue());
        if (convTupleToMap(value, fields, map) == Status.OK) {
          result.add(map);
        } else {
          logger.error("Error scanning keys: from {} to {} limit {} ", startRowKey, endRowKey, recordcount);
          return Status.ERROR;
        }
      }
      return Status.OK;
    } catch (FDBException e) {
      logger.error(MessageFormatter.format("Error scanning keys: from {} to {} ",
          startRowKey, endRowKey).getMessage(), e);
      e.printStackTrace();
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error scanning keys: from {} to {} ",
          startRowKey, endRowKey).getMessage(), e);
      e.printStackTrace();
    }
    return Status.ERROR;
  }
}
