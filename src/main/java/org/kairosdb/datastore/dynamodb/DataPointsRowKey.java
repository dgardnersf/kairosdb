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

import java.util.SortedMap;
import java.util.TreeMap;

public class DataPointsRowKey
{
  private String m_metricName;
  private long m_timestamp;
  private SortedMap<String, String> m_tags;

  public DataPointsRowKey(String metricName, long timestamp)
  {
    this(metricName, timestamp, new TreeMap<String, String>());
  }

  public DataPointsRowKey(String metricName, long timestamp,
      SortedMap<String, String> tags)
  {
    m_metricName = metricName;
    m_timestamp = timestamp;
    m_tags = tags;
  }

  public void addTag(String name, String value)
  {
    m_tags.put(name, value);
  }

  public String getMetricName()
  {
    return m_metricName;
  }

  public SortedMap<String, String> getTags()
  {
    return m_tags;
  }

  public long getTimestamp()
  {
    return m_timestamp;
  }

  public String generateTagString()
  {
    StringBuilder sb = new StringBuilder();
    for (String key : m_tags.keySet())
    {
      sb.append(key).append("=");
      sb.append(m_tags.get(key)).append(":");
    }

    return (sb.toString());
  }

  public void extractTags(String tagString)
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

          m_tags.put(tag, value);
          tag = null;
        }
      }
    }
  }

  @SuppressWarnings("RedundantIfStatement")
  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DataPointsRowKey that = (DataPointsRowKey) o;

    if (m_timestamp != that.m_timestamp) return false;
    if (!m_metricName.equals(that.m_metricName)) return false;
    if (!m_tags.equals(that.m_tags)) return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = m_metricName.hashCode();
    result = 31 * result + (int) (m_timestamp ^ (m_timestamp >>> 32));
    result = 31 * result + m_tags.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "DataPointsRowKey{" +
        "m_metricName='" + m_metricName + '\'' +
        ", m_timestamp=" + m_timestamp +
        ", m_tags=" + m_tags +
        '}';
  }
}
