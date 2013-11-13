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

import org.kairosdb.core.formatter.FormatterException;

import java.io.StringWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class TagGroupByResult implements GroupByResult
{
	private Map<String, String> tagResults = new HashMap<String, String>();
	private TagGroupBy groupBy;
  private JsonFactory jsonFactory = new JsonFactory();

	public TagGroupByResult(TagGroupBy groupBy, Map<String, String> tagResults)
	{
		this.groupBy = checkNotNull(groupBy);
		this.tagResults = checkNotNull(tagResults);
	}

	public Map<String, String> getTagResults()
	{
		return tagResults;
	}

	@Override
	public String toJson() throws FormatterException
	{
		StringWriter stringWriter = new StringWriter();

    try
    {
      JsonGenerator writer = jsonFactory.createGenerator(stringWriter);
      writer.writeStartObject();
      writer.writeFieldName("name");
      writer.writeString("tag");
      writer.writeFieldName("tags");
      writer.writeStartArray();
      for (String name : groupBy.getTagNames())
      {
        writer.writeString(name);
      }
      writer.writeEndArray();

      writer.writeFieldName("group");
      writer.writeStartObject();
      for (String tagName : tagResults.keySet())
      {
        writer.writeFieldName(tagName);
        writer.writeString(tagResults.get(tagName));
      }
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
}
