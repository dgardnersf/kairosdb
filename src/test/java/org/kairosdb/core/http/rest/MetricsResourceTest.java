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
package org.kairosdb.core.http.rest;

import io.dropwizard.testing.junit.ResourceTestRule;

import ch.qos.logback.classic.Level;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.inject.*;
import com.google.inject.name.Names;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointListener;
import org.kairosdb.core.DataPointListenerProvider;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.aggregator.AggregatorFactory;
import org.kairosdb.core.aggregator.TestAggregatorFactory;
import org.kairosdb.core.datastore.*;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.groupby.GroupByFactory;
import org.kairosdb.core.groupby.TestGroupByFactory;
import org.kairosdb.core.http.rest.json.GsonParser;
import com.sun.jersey.api.client.ClientResponse;

import org.kairosdb.util.LoggingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static junit.framework.Assert.assertEquals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;

public class MetricsResourceTest
{
	public static final Logger logger = LoggerFactory.getLogger(MetricsResourceTest.class);

	private static final String ADD_METRIC_URL = "/api/v1/datapoints";
	private static final String GET_METRIC_URL = "/api/v1/datapoints/query";
	private static final String METRIC_NAMES_URL = "/api/v1/metricnames";
	private static final String TAG_NAMES_URL = "/api/v1/tagnames";
	private static final String TAG_VALUES_URL = "/api/v1/tagvalues";

	private static TestDatastore datastore;
	private static QueryQueuingManager queuingManager;

  @Rule
  public ResourceTestRule resources;

  public MetricsResourceTest() 
  {
    try {
      datastore = new TestDatastore();
    } catch (DatastoreException e) {}

    queuingManager = new QueryQueuingManager(3, "localhost");

    Injector injector = Guice.createInjector(new AbstractModule()
        {
          @Override
          protected void configure()
          {
            bind(Datastore.class).toInstance(datastore);
            bind(KairosDatastore.class).in(Singleton.class);
            bind(AggregatorFactory.class).to(TestAggregatorFactory.class);
            bind(GroupByFactory.class).to(TestGroupByFactory.class);
            bind(GsonParser.class).in(Singleton.class);
            bind(new TypeLiteral<List<DataPointListener>>(){}).toProvider(DataPointListenerProvider.class);
            bind(QueryQueuingManager.class).toInstance(queuingManager);
            bindConstant().annotatedWith(Names.named("HOSTNAME")).to("HOST");
            bindConstant().annotatedWith(Names.named("kairosdb.datastore.concurrentQueryThreads")).to(1);
          }
    });

    resources = ResourceTestRule.builder().addResource(injector
        .getInstance(MetricsResource.class)).build();
  }

	@Test
	public void testAddEmptyBody() throws Exception
	{
		ClientResponse response = resources.client().resource(ADD_METRIC_URL).post(ClientResponse.class, "");

		assertResponse(response, 400, "{\"errors\":[\"Invalid json. No content due to end of input.\"]}");
	}

	@Test
	public void testAddSingleMetricLongValueSuccess() throws Exception
	{
		String json = Resources.toString(Resources.getResource("single-metric-long.json"), Charsets.UTF_8);

		ClientResponse response = resources.client().resource(ADD_METRIC_URL).post(ClientResponse.class, json);

		assertResponse(response, 204);
	}

	@Test
	public void testAddSingleMetricDoubleValueSuccess() throws Exception
	{
		String json = Resources.toString(Resources.getResource("single-metric-double.json"), Charsets.UTF_8);

		ClientResponse response = resources.client().resource(ADD_METRIC_URL).post(ClientResponse.class, json);

		assertResponse(response, 204);
	}

	@Test
	public void testAddMutipleDatapointSuccess() throws Exception
	{
		String json = Resources.toString(Resources.getResource("multiple-datapoints-metric.json"), Charsets.UTF_8);

		ClientResponse response = resources.client().resource(ADD_METRIC_URL).post(ClientResponse.class, json);

		assertResponse(response, 204);
	}

	@Test
	public void testAddMultipleMetricLongValueSuccess() throws Exception
	{
		String json = Resources.toString(Resources.getResource("multi-metric-long.json"), Charsets.UTF_8);

		ClientResponse response = resources.client().resource(ADD_METRIC_URL).post(ClientResponse.class, json);

		assertThat(response.getStatus(), equalTo(204));
	}

	@Test
	public void testAddMissingName() throws Exception
	{
		String json = Resources.toString(Resources.getResource("single-metric-missing-name.json"), Charsets.UTF_8);

		ClientResponse response = resources.client().resource(ADD_METRIC_URL).post(ClientResponse.class, json);

		assertResponse(response, 400, "{\"errors\":[\"metric[0].name may not be empty.\"]}");
	}

	@Test
	public void testAddInvalidTimestamp() throws Exception
	{
		String json = Resources.toString(Resources.getResource("multi-metric-invalid-timestamp.json"), Charsets.UTF_8);

		ClientResponse response = resources.client().resource(ADD_METRIC_URL).post(ClientResponse.class, json);

		assertResponse(response, 400, "{\"errors\":[\"datapoints.timestamp must be greater than or equal to 1.\"]}");
	}

	@Test
	public void testQuery() throws IOException
	{
		String json = Resources.toString(Resources.getResource("query-metric-absolute-dates.json"), Charsets.UTF_8);

		ClientResponse response = resources.client().resource(GET_METRIC_URL).post(ClientResponse.class, json);

		assertResponse(response, 200,
				"{\"queries\":" +
						"[{\"sample_size\":10,\"results\":" +
						"[{\"name\":\"abc.123\",\"tags\":{\"server\":[\"server1\",\"server2\"]},\"values\":[[1,60.2],[2,30.200000000000003],[3,20.1]]}]}]}");
	}

	@Test
	public void testQueryWithBeanValidationException() throws IOException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-relative-unit.json"), Charsets.UTF_8);

		ClientResponse response = resources.client().resource(GET_METRIC_URL).post(ClientResponse.class, json);

		assertResponse(response, 400,
				"{\"errors\":[\"query.bogus is not a valid time unit, must be one of MILLISECONDS,SECONDS,MINUTES,HOURS,DAYS,WEEKS,MONTHS,YEARS\"]}");
	}

	@Test
	public void testQueryWithJsonMapperParsingException() throws IOException
	{
		String json = Resources.toString(Resources.getResource("invalid-query-metric-json.json"), Charsets.UTF_8);

		ClientResponse response = resources.client().resource(GET_METRIC_URL).post(ClientResponse.class, json);

		assertResponse(response, 400,
        "{\"errors\":[\"com.google.gson.stream.MalformedJsonException: Use JsonReader.setLenient(true) to accept malformed JSON at line 2 column 22\"]}");
	}

	@Test
	public void testMetricNames() throws IOException
	{
		ClientResponse response = resources.client().resource(METRIC_NAMES_URL).get(ClientResponse.class);

		assertResponse(response, 200, "{\"results\":[\"cpu\",\"memory\",\"disk\",\"network\"]}");
	}

	@Test
	public void testTagNames() throws IOException
	{
		ClientResponse response = resources.client().resource(TAG_NAMES_URL).get(ClientResponse.class);

		assertResponse(response, 200, "{\"results\":[\"server1\",\"server2\",\"server3\"]}");
	}

	@Test
	public void testTagValues() throws IOException
	{
		ClientResponse response = resources.client().resource(TAG_VALUES_URL).get(ClientResponse.class);

		assertResponse(response, 200, "{\"results\":[\"larry\",\"moe\",\"curly\"]}");
	}

	@Test
	public void test_datastoreThrowsException() throws DatastoreException, IOException
	{
		Level previousLogLevel = LoggingUtils.setLogLevel(Level.OFF);

		try
		{
			datastore.throwQueryException(new DatastoreException("bogus"));

			String json = Resources.toString(Resources.getResource("query-metric-absolute-dates.json"), Charsets.UTF_8);

			ClientResponse response = resources.client().resource(GET_METRIC_URL).post(ClientResponse.class, json);

			datastore.throwQueryException(null);

			assertThat(response.getStatus(), equalTo(500));
			assertThat(response.getEntity(String.class), equalTo("{\"errors\":[\"org.kairosdb.core.exception.DatastoreException: bogus\"]}"));
			assertEquals(3, queuingManager.getAvailableThreads());
		}
		finally
		{
			LoggingUtils.setLogLevel(previousLogLevel);
		}
	}

	private void assertResponse(ClientResponse response, int responseCode, String expectedContent)
	{
		assertThat(response.getStatus(), equalTo(responseCode));
		assertThat(response.getHeaders().get("Content-Type").get(0), startsWith("application/json"));
		assertThat(response.getEntity(String.class), equalTo(expectedContent));
	}

	private void assertResponse(ClientResponse response, int responseCode)
	{
		assertThat(response.getStatus(), equalTo(responseCode));
		assertThat(response.getHeaders().get("Content-Type").get(0), startsWith("application/json"));
		assertThat(response.getClientResponseStatus().getReasonPhrase(), equalTo("No Content"));
	}

	public static class TestDatastore implements Datastore
	{
		private DatastoreException m_toThrow = null;

		protected TestDatastore() throws DatastoreException
		{
		}

		public void throwQueryException(DatastoreException toThrow)
		{
			m_toThrow = toThrow;
		}

		@Override
		public void close() throws InterruptedException
		{
		}

		@Override
		public void putDataPoints(DataPointSet dps)
		{

		}

		@Override
		public Iterable<String> getMetricNames()
		{
			return Arrays.asList("cpu", "memory", "disk", "network");
		}

		@Override
		public Iterable<String> getTagNames()
		{
			return Arrays.asList("server1", "server2", "server3");
		}

		@Override
		public Iterable<String> getTagValues()
		{
			return Arrays.asList("larry", "moe", "curly");
		}

		@Override
		public void queryDatabase(DatastoreMetricQuery query, QueryCallback queryCallback) throws DatastoreException
		{
			if (m_toThrow != null)
				throw m_toThrow;

			try
			{
				Map<String, String> tags = new TreeMap<String, String>();
				tags.put("server", "server1");

				queryCallback.startDataPointSet(tags);
				queryCallback.addDataPoint(1, 10);
				queryCallback.addDataPoint(1, 20);
				queryCallback.addDataPoint(2, 10);
				queryCallback.addDataPoint(2, 5);
				queryCallback.addDataPoint(3, 10);

				tags = new TreeMap<String, String>();
				tags.put("server", "server2");

				queryCallback.startDataPointSet(tags);
				queryCallback.addDataPoint(1, 10.1);
				queryCallback.addDataPoint(1, 20.1);
				queryCallback.addDataPoint(2, 10.1);
				queryCallback.addDataPoint(2, 5.1);
				queryCallback.addDataPoint(3, 10.1);

				queryCallback.endDataPoints();
			}
			catch (IOException e)
			{
				throw new DatastoreException(e);
			}
		}

		@Override
		public void deleteDataPoints(DatastoreMetricQuery deleteQuery) throws DatastoreException
		{
		}

		@Override
		public TagSet queryMetricTags(DatastoreMetricQuery query) throws DatastoreException
		{
			return null;
		}
	}
}
