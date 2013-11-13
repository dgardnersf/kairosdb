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

package org.kairosdb.core.http.rest.json;


import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.core.http.rest.validation.TimeUnitRequired;
import org.hibernate.validator.constraints.NotEmpty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class Sampling
{
	@Min(1)
	private int duration;

	@TimeUnitRequired
	private String unit;

	@NotNull
	@NotEmpty()
	private String aggregate;

	@JsonCreator
	public Sampling(@JsonProperty("duration") int duration,
	                @JsonProperty("unit") String unit,
	                @JsonProperty("aggregate") String aggregate)
	{
		this.duration = duration;
		this.unit = unit;
		this.aggregate = aggregate;
	}

	public int getDuration()
	{
		return duration;
	}

	public TimeUnit getUnit()
	{
		return TimeUnit.from(unit);
	}

	public String getAggregate()
	{
		return aggregate;
	}
}
