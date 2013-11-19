package org.kairosdb.datastore.dynamodb;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.*;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;

import com.google.inject.name.Named;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datastore.*;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.datastore.dynamodb.DataCache;
import org.kairosdb.datastore.dynamodb.WriteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamoDBDatastore implements Datastore
{
  public static final Logger logger = LoggerFactory.getLogger(DynamoDBDatastore.class);
  public static final Charset UTF8 = Charset.forName("UTF-8");

  public static final int ROW_KEY_CACHE_SIZE = 1024;
  public static final int STRING_CACHE_SIZE = 1024;

  public static final byte LONG_TYPE = 0x0;
  public static final byte FLOAT_TYPE = 0x1;

  public static final long ROW_WIDTH = 1814400000L; //3 Weeks wide

  public static String TABLE_NAME_DATA_POINTS = "data-points";
  public static String TABLE_NAME_ROW_KEY_INDEX = "row-key-index";
  public static String TABLE_NAME_TAG_NAMES = "tag-names";
  public static String TABLE_NAME_TAG_VALUES = "tag-values";
  public static String TABLE_NAME_METRIC_NAMES = "metric-names";

  public static String ATTR_ROW_KEY = "metric-tbase-tags";
  public static String ATTR_TOFFSET = "toffset";
  public static String ATTR_TYPE    = "type";
  public static String ATTR_VALUE   = "value";
  public static String ATTR_METRIC  = "metric";
  public static String ATTR_TBASE   = "tbase";
  public static String ATTR_TAGS    = "tags";

  public static String ATTR_NAME = "name";

  private AmazonDynamoDBClient m_client;

  private WriteBuffer m_dataPointWriteBuffer;
  private WriteBuffer m_rowKeyWriteBuffer;
  private WriteBuffer m_tagNameWriteBuffer;
  private WriteBuffer m_tagValueWriteBuffer;
  private WriteBuffer m_metricNameWriteBuffer;

  private DataCache<DataPointsRowKey> m_rowKeyCache = new DataCache<DataPointsRowKey>(ROW_KEY_CACHE_SIZE);
  private DataCache<String> m_metricNameCache = new DataCache<String>(STRING_CACHE_SIZE);
  private DataCache<String> m_tagNameCache = new DataCache<String>(STRING_CACHE_SIZE);
  private DataCache<String> m_tagValueCache = new DataCache<String>(STRING_CACHE_SIZE);

  public static final String WRITE_DELAY_PROPERTY = "kairosdb.datastore.dynamodb.write_delay";
  public static final String WRITE_BUFFER_SIZE = "kairosdb.datastore.dynamodb.write_buffer_max_size";

  public DynamoDBDatastore(@Named(WRITE_DELAY_PROPERTY) int writeDelay,
      @Named(WRITE_BUFFER_SIZE) int maxWriteSize,
      final @Named("HOSTNAME") String hostname)
       {

    m_client = new AmazonDynamoDBClient(new BasicAWSCredentials("AKIAID7BMXN7TR4IBMNA",
          "yeXdWCRHvHCpJRNbbOHLyQulWjHKU+GzH8kR0NDv") );

    createSchema();

    ReentrantLock mutatorLock = new ReentrantLock();
    Condition lockCondition = mutatorLock.newCondition();

    m_dataPointWriteBuffer = new WriteBuffer(m_client,
        TABLE_NAME_DATA_POINTS, writeDelay, maxWriteSize,
        new WriteBufferStats()
        {
          @Override
          public void saveWriteSize(int pendingWrites)
          {
            DataPointSet dps = new DataPointSet("kairosdb.datastore.write_size");
            dps.addTag("host", hostname);
            dps.addTag("buffer", TABLE_NAME_DATA_POINTS);
            dps.addDataPoint(new DataPoint(System.currentTimeMillis(), pendingWrites));
            putInternalDataPoints(dps);
          }
    }, mutatorLock, lockCondition);

    m_rowKeyWriteBuffer = new WriteBuffer(m_client,
       TABLE_NAME_ROW_KEY_INDEX, writeDelay, maxWriteSize,
        new WriteBufferStats()
        {
          @Override
          public void saveWriteSize(int pendingWrites)
          {
            DataPointSet dps = new DataPointSet("kairosdb.datastore.write_size");
            dps.addTag("host", hostname);
            dps.addTag("buffer", TABLE_NAME_ROW_KEY_INDEX);
            dps.addDataPoint(new DataPoint(System.currentTimeMillis(), pendingWrites));
            putInternalDataPoints(dps);
          }
    }, mutatorLock, lockCondition);

    m_tagNameWriteBuffer = new WriteBuffer(m_client,
       TABLE_NAME_TAG_NAMES, writeDelay, maxWriteSize,
        new WriteBufferStats()
        {
          @Override
          public void saveWriteSize(int pendingWrites)
          {
            DataPointSet dps = new DataPointSet("kairosdb.datastore.write_size");
            dps.addTag("host", hostname);
            dps.addTag("buffer", TABLE_NAME_TAG_NAMES);
            dps.addDataPoint(new DataPoint(System.currentTimeMillis(), pendingWrites));
            putInternalDataPoints(dps);
          }
    }, mutatorLock, lockCondition);

    m_tagValueWriteBuffer = new WriteBuffer(m_client,
       TABLE_NAME_TAG_VALUES, writeDelay, maxWriteSize,
        new WriteBufferStats()
        {
          @Override
          public void saveWriteSize(int pendingWrites)
          {
            DataPointSet dps = new DataPointSet("kairosdb.datastore.write_size");
            dps.addTag("host", hostname);
            dps.addTag("buffer", TABLE_NAME_TAG_VALUES);
            dps.addDataPoint(new DataPoint(System.currentTimeMillis(), pendingWrites));
            putInternalDataPoints(dps);
          }
    }, mutatorLock, lockCondition);

    m_metricNameWriteBuffer = new WriteBuffer(m_client,
       TABLE_NAME_METRIC_NAMES, writeDelay, maxWriteSize,
        new WriteBufferStats()
        {
          @Override
          public void saveWriteSize(int pendingWrites)
          {
            DataPointSet dps = new DataPointSet("kairosdb.datastore.write_size");
            dps.addTag("host", hostname);
            dps.addTag("buffer", TABLE_NAME_METRIC_NAMES);
            dps.addDataPoint(new DataPoint(System.currentTimeMillis(), pendingWrites));
            putInternalDataPoints(dps);
          }
    }, mutatorLock, lockCondition);
  }

  public void close() throws InterruptedException, DatastoreException {
  }

  @Override
  public void putDataPoints(DataPointSet dps) throws DatastoreException
  {
    try
    {
      long rowTime = -1L;
      DataPointsRowKey rowKey = null;
      //time the data is written.
      long writeTime = System.currentTimeMillis();

      for (DataPoint dp : dps.getDataPoints())
      {
        if (dp.getTimestamp() < 0)
          throw new DatastoreException("Timestamp must be greater than or equal to zero.");
        long newRowTime = calculateRowTime(dp.getTimestamp());
        if (newRowTime != rowTime)
        {
          rowTime = newRowTime;
          rowKey = new DataPointsRowKey(dps.getName(), rowTime, dps.getTags());

          long now = System.currentTimeMillis();
          //Write out the row key if it is not cached
          if (!m_rowKeyCache.isCached(rowKey))
            m_rowKeyWriteBuffer.addData(newRowKey(rowKey));

          //Write metric name if not in cache
          if (!m_metricNameCache.isCached(dps.getName()))
            m_metricNameWriteBuffer.addData(newMetricName(dps.getName()));

          //Check tag names and values to write them out
          Map<String, String> tags = dps.getTags();
          for (String tagName : tags.keySet())
          {
            if (!m_tagNameCache.isCached(tagName))
              m_tagNameWriteBuffer.addData(newTagName(tagName));

            String value = tags.get(tagName);
            if (!m_tagValueCache.isCached(value))
              m_tagValueWriteBuffer.addData(newTagValue(value));
          }
        }

        int toffset = (int) (dp.getTimestamp() - rowTime);

        if (dp.isInteger())
          m_dataPointWriteBuffer.addData(newDatapoint(rowKey, toffset, dp.getLongValue()));
        else
          m_dataPointWriteBuffer.addData(newDatapoint(rowKey, toffset, (float) dp.getDoubleValue()));
      }
    }
    catch (DatastoreException e)
    {
      throw e;
    }
    catch (Exception e)
    {
      throw new DatastoreException(e);
    }
  }

  private ByteBuffer toByteBuffer(DataPointsRowKey dataPointsRowKey)
  {
    int size = 8; //size of timestamp
    byte[] metricName = dataPointsRowKey.getMetricName().getBytes(UTF8);
    size += metricName.length;
    size++; //Add one for null at end of string
    byte[] tagString = generateTagString(dataPointsRowKey.getTags()).getBytes(UTF8);
    size += tagString.length;

    ByteBuffer buffer = ByteBuffer.allocate(size);
    buffer.put(metricName);
    buffer.put((byte)0x0);
    buffer.putLong(dataPointsRowKey.getTimestamp());
    buffer.put(tagString);

    buffer.flip();

    return buffer;
  }

  private String generateTagString(Map<String, String> tags)
  {
    StringBuilder sb = new StringBuilder();
    for (String key : tags.keySet())
    {
      sb.append(key).append("=");
      sb.append(tags.get(key)).append(":");
    }

    return (sb.toString());
  }

  private void extractTags(DataPointsRowKey rowKey, String tagString)
  {
    int mark = 0;
    int position = 0;
    String tag = null;
    String value;

    for (position = 0; position < tagString.length(); position ++)
    {
      if (tag == null)
      {
        if (tagString.charAt(position) == '=')
        {
          tag = tagString.substring(mark, position);
          mark = position +1;
        }
      }
      else
      {
        if (tagString.charAt(position) == ':')
        {
          value = tagString.substring(mark, position);
          mark = position +1;

          rowKey.addTag(tag, value);
          tag = null;
        }
      }
    }
  }

  public DataPointsRowKey fromByteBuffer(ByteBuffer byteBuffer)
  {
    int start = byteBuffer.position();
    byteBuffer.mark();
    //Find null
    while (byteBuffer.get() != 0x0);

    int nameSize = (byteBuffer.position() - start) -1;
    byteBuffer.reset();

    byte[] metricName = new byte[nameSize];
    byteBuffer.get(metricName);
    byteBuffer.get(); //Skip the null

    long timestamp = byteBuffer.getLong();

    DataPointsRowKey rowKey = new DataPointsRowKey(new String(metricName, UTF8),
        timestamp);

    byte[] tagString = new byte[byteBuffer.remaining()];
    byteBuffer.get(tagString);

    String tags = new String(tagString, UTF8);

    extractTags(rowKey, tags);

    return rowKey;
  }

  private Map<String, AttributeValue> newMetricName(String name) {
    Map<String, AttributeValue> item = new HashMap<String, AttributeValue>(1);
    item.put(ATTR_NAME, new AttributeValue().withS(name));
    return item;
  }

  private Map<String, AttributeValue> newTagValue(String value) {
    Map<String, AttributeValue> item = new HashMap<String, AttributeValue>(1);
    item.put(ATTR_VALUE, new AttributeValue().withS(value));
    return item;
  }

  private Map<String, AttributeValue> newTagName(String name) {
    Map<String, AttributeValue> item = new HashMap<String, AttributeValue>(1);
    item.put(ATTR_NAME, new AttributeValue().withS(name));
    return item;
  }

  private Map<String, AttributeValue> newRowKey(DataPointsRowKey rowKey) {
    return newRowKey(rowKey.getMetricName(), rowKey.getTimestamp(), rowKey.getTags());
  }

  private Map<String, AttributeValue> newRowKey(String metricName, long baseTime,
      Map<String, String> tags) {
    Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
    item.put(ATTR_METRIC, new AttributeValue(metricName));
    item.put(ATTR_TBASE, new AttributeValue().withN(Long.toString(baseTime)));
    item.put(ATTR_TAGS, new AttributeValue(generateTagString(tags)));
    return item;
  }

  private Map<String, AttributeValue> newDatapoint(DataPointsRowKey rowKey, long offsetTime, long value) {
    Map<String, AttributeValue> item = newDatapoint(rowKey, offsetTime);
    item.put(ATTR_TYPE, new AttributeValue().withN(Long.toString(LONG_TYPE)));
    item.put(ATTR_VALUE, new AttributeValue().withN(Long.toString(value)));
    return item;
  }

  private Map<String, AttributeValue> newDatapoint(DataPointsRowKey rowKey, long offsetTime, float value) {
    Map<String, AttributeValue> item = newDatapoint(rowKey, offsetTime);
    item.put(ATTR_TYPE, new AttributeValue().withN(Long.toString(FLOAT_TYPE)));
    item.put(ATTR_VALUE, new AttributeValue().withN(Float.toString(value)));
    return item;
  }

  private Map<String, AttributeValue> newDatapoint(DataPointsRowKey rowKey, long offsetTime) {
    Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
    item.put(ATTR_ROW_KEY, new AttributeValue().withB(toByteBuffer(rowKey)));
    item.put(ATTR_TOFFSET, new AttributeValue().withN(Long.toString(offsetTime)));
    return item;
  }

  public Iterable<String> getMetricNames() throws DatastoreException {
    return null;
  }

  public Iterable<String> getTagNames() throws DatastoreException {
    return null;
  }

  public Iterable<String> getTagValues() throws DatastoreException {
    return null;
  }

  public void queryDatabase(DatastoreMetricQuery query, QueryCallback queryCallback) throws DatastoreException {
  }

  public void deleteDataPoints(DatastoreMetricQuery deleteQuery) throws DatastoreException {
  }

  public TagSet queryMetricTags(DatastoreMetricQuery query) throws DatastoreException {
    return null;
  }

  private void putInternalDataPoints(DataPointSet dps)
  {
    try
    {
      putDataPoints(dps);
    }
    catch (DatastoreException e)
    {
      logger.error("", e);
    }
  }

  private void createSchema()
  {
    if (!tableExists(m_client, TABLE_NAME_DATA_POINTS))
      createTable(m_client, new CreateTableRequest()
          .withTableName(TABLE_NAME_DATA_POINTS)
          .withKeySchema(new KeySchemaElement()
            .withAttributeName(ATTR_ROW_KEY).withKeyType(KeyType.HASH))
          .withKeySchema(new KeySchemaElement()
            .withAttributeName(ATTR_TOFFSET).withKeyType(KeyType.RANGE))
          .withAttributeDefinitions( new AttributeDefinition()
            .withAttributeName(ATTR_ROW_KEY).withAttributeType(ScalarAttributeType.B),
            new AttributeDefinition()
            .withAttributeName(ATTR_TOFFSET).withAttributeType(ScalarAttributeType.N),
            new AttributeDefinition()
            .withAttributeName(ATTR_TYPE).withAttributeType(ScalarAttributeType.S),
            new AttributeDefinition()
            .withAttributeName(ATTR_VALUE).withAttributeType(ScalarAttributeType.N) )
          .withProvisionedThroughput(new ProvisionedThroughput()
            .withReadCapacityUnits(1L)
            .withWriteCapacityUnits(1L)));

    if (!tableExists(m_client, TABLE_NAME_ROW_KEY_INDEX))
      createTable(m_client, new CreateTableRequest()
          .withTableName(TABLE_NAME_ROW_KEY_INDEX)
          .withKeySchema(new KeySchemaElement()
            .withAttributeName(ATTR_METRIC).withKeyType(KeyType.HASH))
          .withKeySchema(new KeySchemaElement()
            .withAttributeName(ATTR_TBASE).withKeyType(KeyType.RANGE))
          .withAttributeDefinitions( new AttributeDefinition()
            .withAttributeName(ATTR_METRIC).withAttributeType(ScalarAttributeType.S),
            new AttributeDefinition()
            .withAttributeName(ATTR_TBASE).withAttributeType(ScalarAttributeType.N),
            new AttributeDefinition()
            .withAttributeName(ATTR_TAGS).withAttributeType(ScalarAttributeType.S) )
          .withProvisionedThroughput(new ProvisionedThroughput()
            .withReadCapacityUnits(1L)
            .withWriteCapacityUnits(1L)));

    if (!tableExists(m_client, TABLE_NAME_TAG_NAMES))
      createTable(m_client, new CreateTableRequest()
          .withTableName(TABLE_NAME_TAG_NAMES)
          .withKeySchema(new KeySchemaElement()
            .withAttributeName(ATTR_NAME).withKeyType(KeyType.HASH))
          .withAttributeDefinitions( new AttributeDefinition()
            .withAttributeName(ATTR_NAME).withAttributeType(ScalarAttributeType.S))
          .withProvisionedThroughput(new ProvisionedThroughput()
            .withReadCapacityUnits(1L)
            .withWriteCapacityUnits(1L)));

    if (!tableExists(m_client, TABLE_NAME_TAG_VALUES))
      createTable(m_client, new CreateTableRequest()
          .withTableName(TABLE_NAME_TAG_VALUES)
          .withKeySchema(new KeySchemaElement()
            .withAttributeName(ATTR_VALUE).withKeyType(KeyType.HASH))
          .withAttributeDefinitions( new AttributeDefinition()
            .withAttributeName(ATTR_VALUE).withAttributeType(ScalarAttributeType.S))
          .withProvisionedThroughput(new ProvisionedThroughput()
            .withReadCapacityUnits(1L)
            .withWriteCapacityUnits(1L)));

    if (!tableExists(m_client, TABLE_NAME_METRIC_NAMES))
      createTable(m_client, new CreateTableRequest()
          .withTableName(TABLE_NAME_METRIC_NAMES)
          .withKeySchema(new KeySchemaElement()
            .withAttributeName(ATTR_NAME).withKeyType(KeyType.HASH))
          .withAttributeDefinitions( new AttributeDefinition()
            .withAttributeName(ATTR_NAME).withAttributeType(ScalarAttributeType.S))
          .withProvisionedThroughput(new ProvisionedThroughput()
            .withReadCapacityUnits(1L)
            .withWriteCapacityUnits(1L)));
  }

  public static long calculateRowTime(long timestamp)
  {
    return (timestamp - (timestamp % ROW_WIDTH));
  }

  private static void createTable(AmazonDynamoDBClient client, CreateTableRequest createTableRequest) {
    TableDescription createdTableDescription = client.createTable(createTableRequest).getTableDescription();
    System.out.println("Created Table: " + createdTableDescription);
    // Wait for it to become active
    waitForTableToBecomeAvailable(client, createTableRequest.getTableName());
  }

  private boolean tableExists(AmazonDynamoDBClient client, String tableName) {
    try {
      DescribeTableRequest request = new DescribeTableRequest().withTableName(tableName);
    } catch (AmazonServiceException ase) {
      if (ase.getErrorCode().equalsIgnoreCase("ResourceNotFoundException"))
        return false;
      else
        throw ase;
    }
    return true;
  }

  private static void waitForTableToBecomeAvailable(AmazonDynamoDBClient client, String tableName) {
    System.out.println("Waiting for " + tableName + " to become ACTIVE...");

    long startTime = System.currentTimeMillis();
    long endTime = startTime + (10 * 60 * 1000);
    while (System.currentTimeMillis() < endTime) {
      try {Thread.sleep(1000 * 20);} catch (Exception e) {}
      try {
        DescribeTableRequest request = new DescribeTableRequest().withTableName(tableName);
        TableDescription tableDescription = client.describeTable(request).getTable();
        String tableStatus = tableDescription.getTableStatus();
        System.out.println("  - current state: " + tableStatus);
        if (tableStatus.equals(TableStatus.ACTIVE.toString())) return;
      } catch (AmazonServiceException ase) {
        if (ase.getErrorCode().equalsIgnoreCase("ResourceNotFoundException") == false) throw ase;
      }
    }
    throw new RuntimeException("Table " + tableName + " never went active");
  }
}
