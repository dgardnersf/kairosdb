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
import org.kairosdb.core.datastore.Duration;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.core.formatter.FormatterException;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.io.StringWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@GroupByName(name = "time", description = "Groups data points in time ranges.")
public class TimeGroupBy implements GroupBy
{
	@NotNull
	private Duration rangeSize;

	@Min(1)
	private int groupCount;

	private long startDate;
	private Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

  private JsonFactory jsonFactory = new JsonFactory();


	public TimeGroupBy()
	{
	}

	public TimeGroupBy(Duration rangeSize, int groupCount)
	{
		checkArgument(groupCount > 0);

		this.rangeSize = checkNotNull(rangeSize);
		this.groupCount = groupCount;
	}

	@Override
	public int getGroupId(DataPoint dataPoint, Map<String, String> tags)
	{
		if (rangeSize.getUnit() == TimeUnit.MONTHS)
		{
			calendar.setTimeInMillis(dataPoint.getTimestamp());
			int dataPointYear = calendar.get(Calendar.YEAR);
			int dataPointMonth = calendar.get(Calendar.MONTH);

			calendar.setTimeInMillis(startDate);
			int startDateYear = calendar.get(Calendar.YEAR);
			int startDateMonth = calendar.get(Calendar.MONTH);

			return ((dataPointYear - startDateYear) * 12 + (dataPointMonth - startDateMonth)) % groupCount;
		}
		else
	 	    return (int) (((dataPoint.getTimestamp() - startDate) / convertGroupSizeToMillis() ) % groupCount);
	}

	@SuppressWarnings("NumericOverflow")
	private long convertGroupSizeToMillis()
	{
		long milliseconds = rangeSize.getValue();
		switch(rangeSize.getUnit())
		{
			case YEARS: milliseconds *= 52;
			case WEEKS: milliseconds *= 7;
			case DAYS: milliseconds *= 24;
			case HOURS: milliseconds *= 60;
			case MINUTES: milliseconds *= 60;
			case SECONDS: milliseconds *= 1000;
		}

		return milliseconds;
	}

	public void setRangeSize(Duration rangeSize)
	{
		this.rangeSize = rangeSize;
	}

	public void setGroupCount(int groupCount)
	{
		this.groupCount = groupCount;
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
					JsonGenerator writer = jsonFactory.createGenerator(stringWriter);

          writer.writeStartObject();
          writer.writeFieldName("name");
          writer.writeString("time");
          writer.writeFieldName("range_size");
          writer.writeStartObject();
          writer.writeFieldName("value");
          writer.writeNumber(rangeSize.getValue());
          writer.writeFieldName("unit");
          writer.writeString(rangeSize.getUnit().toString());
          writer.writeEndObject();
          writer.writeFieldName("group_count");
          writer.writeNumber(groupCount);
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
		this.startDate = startDate;
	}
}
