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

package org.kairosdb.datastore.cassandra;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.KairosDBConfiguration;

import java.util.HashMap;
import java.util.Map;

public class CassandraModule extends AbstractModule
{
	public static final String CASSANDRA_AUTH_MAP = "cassandra.auth.map";
	public static final String AUTH_PREFIX = "kairosdb.datastore.cassandra.auth.";

	@Override
	protected void configure()
	{
		bind(Datastore.class).to(CassandraDatastore.class).in(Scopes.SINGLETON);
		bind(CassandraDatastore.class).in(Scopes.SINGLETON);
		bind(IncreaseMaxBufferSizesJob.class).in(Scopes.SINGLETON);
	}
}
