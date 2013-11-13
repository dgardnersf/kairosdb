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

import static com.google.common.base.Preconditions.checkNotNull;

public class JsonFormatter implements DataFormatter
{
  private JsonFactory jsonFactory = new JsonFactory();

	@Override
	public void format(Writer writer, Iterable<String> iterable) throws FormatterException
	{
		checkNotNull(writer);
		checkNotNull(iterable);

		try
		{
      JsonGenerator jsonWriter = jsonFactory.createGenerator(writer);
			jsonWriter.writeStartObject();
      jsonWriter.writeFieldName("results");
      jsonWriter.writeStartArray();
			for (String string : iterable)
			{
				jsonWriter.writeString(string);
			}
			jsonWriter.writeEndArray();
      jsonWriter.writeEndObject();
      jsonWriter.flush();
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

	@Override
	public void format(Writer writer, List<List<DataPointGroup>> data) throws FormatterException
	{

		checkNotNull(writer);
		checkNotNull(data);
		try
		{
      JsonGenerator jsonWriter = jsonFactory.createGenerator(writer);

			jsonWriter.writeStartObject();
      jsonWriter.writeFieldName("queries");
      jsonWriter.writeStartArray();

			for (List<DataPointGroup> groups : data)
			{
				jsonWriter.writeStartObject();
        jsonWriter.writeFieldName("results");
        jsonWriter.writeStartArray();

				for (DataPointGroup group : groups)
				{
					final String metric = group.getName();

					jsonWriter.writeStartObject();
					jsonWriter.writeFieldName("name");
          jsonWriter.writeString(metric);

					if (!group.getGroupByResult().isEmpty())
					{
						jsonWriter.writeFieldName("group_by");
						jsonWriter.writeStartArray();
            jsonWriter.flush();   // must flush because below the Writer is used directly
						boolean first = true;
						for (GroupByResult groupByResult : group.getGroupByResult())
						{
							if (!first)
								writer.write(",");
							writer.write(groupByResult.toJson());
							first = false;
						}
						jsonWriter.writeEndArray();
					}

					jsonWriter.writeFieldName("tags");
          jsonWriter.writeStartObject();

					for (String tagName : group.getTagNames())
					{
						jsonWriter.writeFieldName(tagName);
            jsonWriter.writeStartArray();
            for (String tagValue : group.getTagValues(tagName)) {
              jsonWriter.writeString(tagValue);
            }
            jsonWriter.writeEndArray();
					}
					jsonWriter.writeEndObject();

					jsonWriter.writeFieldName("values");
          jsonWriter.writeStartArray();
					while (group.hasNext())
					{
						DataPoint dataPoint = group.next();

						jsonWriter.writeStartArray();
            jsonWriter.writeNumber(dataPoint.getTimestamp());
						if (dataPoint.isInteger())
						{
							jsonWriter.writeNumber(dataPoint.getLongValue());
						}
						else
						{
							final double value = dataPoint.getDoubleValue();
							if (value != value || Double.isInfinite(value))
							{
								throw new IllegalStateException("NaN or Infinity:" + value + " data point=" + dataPoint);
							}
							jsonWriter.writeNumber(value);
						}
						jsonWriter.writeEndArray();
					}
					jsonWriter.writeEndArray();
					jsonWriter.writeEndObject();

					group.close();
				}

				jsonWriter.writeEndArray();
        jsonWriter.writeEndObject();
			}

			jsonWriter.writeEndArray();
      jsonWriter.writeEndObject();
      jsonWriter.flush();
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
