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
package org.kairosdb.core.groupby;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.annotation.GroupByName;
import org.kairosdb.core.formatter.FormatterException;

import javax.validation.constraints.Min;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Groups data points by value. Data points are a range of values specified by range size.
 */
@GroupByName(name = "value", description = "Groups data points by value.")
public class ValueGroupBy implements GroupBy
{
	@Min(1)
	private int rangeSize;

	public ValueGroupBy()
	{
	}

	public ValueGroupBy(int rangeSize)
	{
		checkArgument(rangeSize > 0);

		this.rangeSize = rangeSize;
	}

	@Override
	public int getGroupId(DataPoint dataPoint, Map<String, String> tags)
	{
		if (dataPoint.isInteger())
			return (int) (dataPoint.getLongValue() / rangeSize);
		else
			return (int) dataPoint.getDoubleValue() / rangeSize;
	}

	@Override
	public GroupByResult getGroupByResult(final int id)
	{
		return new GroupByResult()
		{
			@Override
			public String toJson() throws FormatterException
			{
				StringWriter stringWriter = new StringWriter();
				try
				{
					JsonGenerator writer = new JsonFactory().createGenerator(stringWriter);

					writer.writeStartObject();
					writer.writeFieldName("name");
          writer.writeString("value");
					writer.writeFieldName("range_size");
          writer.writeNumber(rangeSize);

					writer.writeFieldName("group");
          writer.writeStartObject();
					writer.writeFieldName("group_number");
          writer.writeNumber(id);
					writer.writeEndObject();
					writer.writeEndObject();
          writer.flush();
				}
				catch (JsonGenerationException e)
				{
					throw new FormatterException(e);
				}
				catch (IOException e)
				{
					throw new FormatterException(e);
				}
				return stringWriter.toString();
			}
		};
	}

	@Override
	public void setStartDate(long startDate)
	{
	}

	public void setRangeSize(int rangeSize)
	{
		this.rangeSize = rangeSize;
	}
}
