package org.kairosdb.core;

import javax.inject.Named;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import org.kairosdb.core.KairosDBConfiguration;
import org.kairosdb.core.datastore.QueryQueuingManager;
import org.kairosdb.core.http.rest.MetricsResource;
import org.kairosdb.core.jobs.CacheFileCleaner;
import org.kairosdb.core.reporting.MetricReporterService;
import org.kairosdb.datastore.cassandra.CassandraDatastore;
import org.kairosdb.datastore.cassandra.CassandraModule;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.datastore.cassandra.IncreaseMaxBufferSizesJob;
import org.kairosdb.util.Util;

import java.util.Map;
import java.util.List;
import java.util.HashMap;

public class KairosDBModule extends AbstractModule {
  @Override
  protected void configure() { 

  }

  @Provides
  @Named(CassandraModule.CASSANDRA_AUTH_MAP)
  public Map<String, String> provideAuthMap(KairosDBConfiguration configuration) {
    Map<String, String> authMap = new HashMap<String, String>();
    authMap.put("username", configuration.getDatastore().getCassandra().getAuth().getUserName());
    authMap.put("password", configuration.getDatastore().getCassandra().getAuth().getPassword());
    return authMap;
  }

  @Provides
  @Named(IncreaseMaxBufferSizesJob.SCHEDULE)
  public String provideIncreaseBufferSizeSchedule(KairosDBConfiguration configuration) {
    return configuration.getDatastore().getCassandra().getIncreaseBufferSizeSchedule();
  }

  @Provides
  @Named("kairosdb.telnetserver.port")
  public int provideTelnetServerPort(KairosDBConfiguration configuration) {
    return configuration.getTelnetServer().getPort();
  }

  @Provides
  @Named(MetricReporterService.SCHEDULE_PROPERTY)
  public String provideReporterSchedule(KairosDBConfiguration configuration) {
    return configuration.getReporter().getSchedule();
  }

  @Provides
  @Named(CacheFileCleaner.CLEANING_SCHEDULE)
  public String provideQueryCacheFileCleanerSchedule(KairosDBConfiguration configuration) {
    return configuration.getQueryCache().getFileCleanerSchedule();
  }

  @Provides
  @Named(QueryQueuingManager.CONCURRENT_QUERY_THREAD)
  public int provideConcurrentQueryThreads(KairosDBConfiguration configuration) {
    return configuration.getDatastore().getConcurrentQueryThreads();
  }

  @Provides
  @Named(KairosDatastore.QUERY_CACHE_DIR)
  public String provideQueryCacheDir(KairosDBConfiguration configuration) {
    return configuration.getQueryCache().getCacheDir();
  }
 
  @Provides
  @Named(CassandraDatastore.HOST_LIST_PROPERTY)
  public String provideHosts(KairosDBConfiguration configuration) {
    return configuration.getDatastore().getCassandra().getHosts();
  }

  @Provides
  @Named(CassandraDatastore.REPLICATION_FACTOR_PROPERTY)
  public int provideReplicationFactor(KairosDBConfiguration configuration) {
    return configuration.getDatastore().getCassandra().getReplicationFactor();
  }

  @Provides
  @Named("HOSTNAME")
  public String provideHostname(KairosDBConfiguration configuration) {
    String hostname = configuration.getHostName();
    return hostname != null ? hostname: Util.getHostName();
  }

  @Provides
  @Named(CassandraDatastore.WRITE_DELAY_PROPERTY)
  public int provideWriteDelay(KairosDBConfiguration configuration) {
    return configuration.getDatastore().getCassandra().getWriteDelay();
  }

  @Provides
  @Named(CassandraDatastore.WRITE_BUFFER_SIZE)
  public int provideWriteBufferMaxSize(KairosDBConfiguration configuration) {
    return configuration.getDatastore().getCassandra().getWriteBufferMaxSize();
  }

  @Provides
  @Named(CassandraDatastore.SINGLE_ROW_READ_SIZE_PROPERTY)
  public int provideSingleRowReadSize(KairosDBConfiguration configuration) {
    return configuration.getDatastore().getCassandra().getSingleRowReadSize();
  }

  @Provides
  @Named(CassandraDatastore.MULTI_ROW_READ_SIZE_PROPERTY)
  public int provideMultiRowReadSize(KairosDBConfiguration configuration) {
    return configuration.getDatastore().getCassandra().getMultiRowReadSize();
  }

  @Provides
  @Named(CassandraDatastore.MULTI_ROW_SIZE_PROPERTY)
  public int provideMultiRowSize(KairosDBConfiguration configuration) {
    return configuration.getDatastore().getCassandra().getMultiRowSize();
  }
}
