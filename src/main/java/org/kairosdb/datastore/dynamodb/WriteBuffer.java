/*
 * Copyright 2013 Proofpoint Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.kairosdb.datastore.dynamodb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;

import org.kairosdb.datastore.dynamodb.WriteBufferStats;

import java.util.LinkedList;
import java.util.Map;

public class WriteBuffer<RowKeyType, ColumnKeyType, ValueType>  implements Runnable
{
  public static final Logger logger = LoggerFactory.getLogger(WriteBuffer.class);

  private String m_tableName;
  private volatile int m_bufferCount = 0;
  private ReentrantLock m_mutatorLock;
  private Condition m_lockCondition;

  private Thread m_writeThread;
  private boolean m_exit = false;
  private int m_writeDelay;
  private WriteBufferStats m_writeStats;
  private int m_maxBufferSize;
  private int m_initialMaxBufferSize;

  private AmazonDynamoDBClient m_client;
  private LinkedList<WriteRequest> m_writeBuffer;

  public WriteBuffer(AmazonDynamoDBClient client, 
      String tableName,
      int writeDelay, int maxWriteSize,
      WriteBufferStats stats,
      ReentrantLock mutatorLock,
      Condition lockCondition)
  {
    m_tableName = tableName;
    m_writeDelay = writeDelay;
    m_initialMaxBufferSize = m_maxBufferSize = maxWriteSize;
    m_writeStats = stats;
    m_mutatorLock = mutatorLock;
    m_lockCondition = lockCondition;

    m_client = client;
    m_writeBuffer = new LinkedList<WriteRequest> ();
    m_writeThread = new Thread(this);
    m_writeThread.start();
  }

  public void addData(Map<String, AttributeValue> entry)
  {
    m_mutatorLock.lock();
    try
    {
      waitOnBufferFull();
      m_bufferCount++;
      m_writeBuffer.add(new WriteRequest(new PutRequest(entry)));
    }
    finally
    {
      m_mutatorLock.unlock();
    }
  }

//  public void deleteRow(RowKeyType rowKey, long timestamp)
//  {
//    m_mutatorLock.lock();
//    try
//    {
//      waitOnBufferFull();
//
//      m_bufferCount++;
//      m_mutator.addDeletion(rowKey, m_tableName, timestamp);
//    }
//    finally
//    {
//      m_mutatorLock.unlock();
//    }
//  }
//
//  public void deleteColumn(RowKeyType rowKey, ColumnKeyType columnKey, long timestamp)
//  {
//    m_mutatorLock.lock();
//    try
//    {
//      waitOnBufferFull();
//
//      m_bufferCount++;
//      m_mutator.addDeletion(rowKey, m_tableName, columnKey, m_columnKeySerializer, timestamp);
////      m_mutator.delete(rowKey, m_tableName, columnKey, m_columnKeySerializer, timestamp);
//    }
//    finally
//    {
//      m_mutatorLock.unlock();
//    }
//  }

  private void waitOnBufferFull()
  {
    if ((m_bufferCount > m_maxBufferSize) && (m_mutatorLock.getHoldCount() == 1))
    {
      try
      {
        m_lockCondition.await();
      }
      catch (InterruptedException ignored) {}
    }
  }

  public void close() throws InterruptedException
  {
    m_exit = true;
    m_writeThread.interrupt();
    m_writeThread.join();
  }

  @Override
  public void run()
  {
    while (!m_exit)
    {
      try
      {
        Thread.sleep(m_writeDelay);
      }
      catch (InterruptedException ignored) {}

      LinkedList<WriteRequest> pending = null;
      
      if (m_bufferCount != 0)
      {
        m_mutatorLock.lock();
        try
        {
          m_writeStats.saveWriteSize(m_bufferCount);

          pending = m_writeBuffer;
          m_writeBuffer = new LinkedList<WriteRequest>();
          m_bufferCount = 0;
          m_lockCondition.signalAll();
        }
        finally
        {
          m_mutatorLock.unlock();
        }
      }

      try
      {
        if (pending != null)
          m_client.batchWriteItem(
              new BatchWriteItemRequest().addRequestItemsEntry(m_tableName, pending));

        pending = null;
      }
      catch (Exception e)
      {
        logger.error("Error sending data to DynamoDB", e);

        m_maxBufferSize = m_maxBufferSize * 3 / 4;

        logger.error("Reducing write buffer size to " + m_maxBufferSize + ".  " + 
            "You need to increase your dynamodb capacity or change the " + 
                "kairosdb.datastore.dynamodb.write_buffer_max_size property.");
      }

      // If the batch failed we will retry it without changing the buffer size.
      while (pending != null)
      {
        try
        {
          Thread.sleep(100);
        }
        catch (InterruptedException ignored){ }

        try
        {
          m_client.batchWriteItem(
              new BatchWriteItemRequest().addRequestItemsEntry(m_tableName, pending));
          pending = null;
        }
        catch (Exception e)
        {
          logger.error("Error resending data to DynamoDB", e);
        }
      }
    }
  }
}
