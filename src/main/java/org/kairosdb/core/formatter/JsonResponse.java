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

package org.kairosdb.core.formatter;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.groupby.GroupByResult;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

public class JsonResponse
{
	private Writer m_writer;
	private JsonGenerator m_jsonWriter;

	public JsonResponse(Writer writer) throws FormatterException
	{
		m_writer = writer;
    try 
    {
      m_jsonWriter = new JsonFactory().createGenerator(writer);
    }
    catch (IOException e) 
    {
			throw new FormatterException(e);
    }
	}

	public void begin() throws FormatterException
	{
		try
		{
			m_jsonWriter.writeStartObject();
			m_jsonWriter.writeFieldName("queries");
      m_jsonWriter.writeStartArray();
		}
		catch (JsonGenerationException e)
		{
			throw new FormatterException(e);
		}
    catch (IOException e) 
    {
			throw new FormatterException(e);
    }
	}

	/**
	 * Formats the query results
	 *
	 * @param queryResults results of the query
	 * @param excludeTags if true do not include tag information
	 * @param sampleSize   Passing a sample size of -1 will cause the attribute to not show up
	 * @throws FormatterException
	 */
	public void formatQuery(List<DataPointGroup> queryResults, boolean excludeTags, int sampleSize) throws FormatterException
	{
		try
		{
			m_jsonWriter.writeStartObject();

			if (sampleSize != -1)
      {
        m_jsonWriter.writeFieldName("sample_size");
        m_jsonWriter.writeNumber(sampleSize);
      }

			m_jsonWriter.writeFieldName("results");
      m_jsonWriter.writeStartArray();

			for (DataPointGroup group : queryResults)
			{
				final String metric = group.getName();

				m_jsonWriter.writeStartObject();
				m_jsonWriter.writeFieldName("name");
        m_jsonWriter.writeString(metric);

				if (!group.getGroupByResult().isEmpty())
				{
					m_jsonWriter.writeFieldName("group_by");
					m_jsonWriter.writeStartArray();
					m_jsonWriter.flush(); // flush here because the Writer is accessed directly (below)
					boolean first = true;
					for (GroupByResult groupByResult : group.getGroupByResult())
					{
						if (!first)
							m_writer.write(",");
						m_writer.write(groupByResult.toJson());
						first = false;
					}
					m_jsonWriter.writeEndArray();
				}

				if (!excludeTags)
				{
					m_jsonWriter.writeFieldName("tags");
          m_jsonWriter.writeStartObject();

					for (String tagName : group.getTagNames())
					{
						m_jsonWriter.writeFieldName(tagName);
            m_jsonWriter.writeStartArray();
            for (String tagValue : group.getTagValues(tagName)) 
            {
              m_jsonWriter.writeString(tagValue);
            }
            m_jsonWriter.writeEndArray();
					}
					m_jsonWriter.writeEndObject();
				}

				m_jsonWriter.writeFieldName("values");
        m_jsonWriter.writeStartArray();
				while (group.hasNext())
				{
					DataPoint dataPoint = group.next();

					m_jsonWriter.writeStartArray();
          m_jsonWriter.writeNumber(dataPoint.getTimestamp());
					if (dataPoint.isInteger())
					{
						m_jsonWriter.writeNumber(dataPoint.getLongValue());
					}
					else
					{
						final double value = dataPoint.getDoubleValue();
						if (value != value || Double.isInfinite(value))
						{
							throw new IllegalStateException("NaN or Infinity:" + value + " data point=" + dataPoint);
						}
						m_jsonWriter.writeNumber(value);
					}
					m_jsonWriter.writeEndArray();
				}
				m_jsonWriter.writeEndArray();
				m_jsonWriter.writeEndObject();
			}

			m_jsonWriter.writeEndArray();
      m_jsonWriter.writeEndObject();
		}
		catch (JsonGenerationException e)
		{
			throw new FormatterException(e);
		}
		catch (IOException e)
		{
			throw new FormatterException(e);
		}
	}

	public void end() throws FormatterException
	{
		try
		{
			m_jsonWriter.writeEndArray();
			m_jsonWriter.writeEndObject();
      m_jsonWriter.flush();
		}
		catch (JsonGenerationException e)
		{
			throw new FormatterException(e);
		}
		catch (IOException e)
		{
			throw new FormatterException(e);
		}
	}
}
