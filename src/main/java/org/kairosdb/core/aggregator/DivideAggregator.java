/*
 * Copyright 2013 Proofpoint Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kairosdb.core.aggregator;


import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.annotation.AggregatorName;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.groupby.GroupByResult;
import org.kairosdb.core.http.rest.validation.NonZero;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

@AggregatorName(name = "div", description = "Divides each data point by a divisor.")
public class DivideAggregator implements Aggregator
{
	@NonZero
	private double m_divisor;

	@Override
	public DataPointGroup aggregate(DataPointGroup dataPointGroup)
	{
		checkState(m_divisor != 0.0);
		return new DivideDataPointGroup(dataPointGroup);
	}

	public void setDivisor(double divisor)
	{
		m_divisor = divisor;
	}

	private class DivideDataPointGroup implements DataPointGroup
	{
		private DataPointGroup m_innerDataPointGroup;

		public DivideDataPointGroup(DataPointGroup innerDataPointGroup)
		{
			m_innerDataPointGroup = innerDataPointGroup;
		}

		@Override
		public boolean hasNext()
		{
			return (m_innerDataPointGroup.hasNext());
		}

		@Override
		public DataPoint next()
		{
			DataPoint dp = m_innerDataPointGroup.next();

			dp = new DataPoint(dp.getTimestamp(), dp.getDoubleValue() / m_divisor);

			return (dp);
		}

		@Override
		public void remove()
		{
			m_innerDataPointGroup.remove();
		}

		@Override
		public String getName()
		{
			return (m_innerDataPointGroup.getName());
		}

		@Override
		public List<GroupByResult> getGroupByResult()
		{
			return (m_innerDataPointGroup.getGroupByResult());
		}

		@Override
		public void close()
		{
			m_innerDataPointGroup.close();
		}

		@Override
		public Set<String> getTagNames()
		{
			return (m_innerDataPointGroup.getTagNames());
		}

		@Override
		public Set<String> getTagValues(String tag)
		{
			return (m_innerDataPointGroup.getTagValues(tag));
		}
	}
}
