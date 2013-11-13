package org.kairosdb.core;

import io.dropwizard.Configuration;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import com.google.common.collect.ImmutableList;
import java.util.List;

public class KairosDBConfiguration extends Configuration {
  public static class TelnetServer {
    @JsonProperty
    private int port;
    @JsonProperty
    private boolean enabled;

    public boolean getEnabled() {
      return enabled;
    }

    public int getPort() {
      return port;
    }
  }

  public static class Reporter {
    @JsonProperty
    private String schedule;
    @JsonProperty
    private boolean enabled;

    public boolean getEnabled() {
      return enabled;
    }

    public String getSchedule() {
      return schedule;
    }
  }

  public static class Service {
    public static class Oauth {
      public static class Consumer {
        private String key;
        private String secret;
        public String getKey() {
          return key;
        }
        public String getSecret() {
          return secret;
        }
      }

      @JsonProperty
      Consumer consumer;
      @JsonProperty
      private boolean enabled;

      public boolean getEnabled() {
        return enabled;
      }

      Consumer getConsumer() {
        return consumer;
      }
    }

    @JsonProperty
    private String datastore;
    @JsonProperty
    private Oauth oauth;

    public Oauth getOauth() {
      return oauth;
    }

    public String getDatastore() {
      return datastore;
    }
  }

  public static class QueryCache {
    @JsonProperty("file_cleaner_schedule")
    String fileCleanerSchedule;
    @JsonProperty("cache_dir")
    String cacheDir;

    public String getFileCleanerSchedule() {
      return fileCleanerSchedule;
    }
    public String getCacheDir() {
      return cacheDir;
    }
  }

  public static class Datastore {
    public static class H2 {
      @JsonProperty("database_path")
      private String databasePath;

      public String getDatabasePath() {
        return databasePath;
      }
    }

    public static class Cassandra {
      public static class Auth {
        @JsonProperty("username")
        private String userName;
        @JsonProperty
        private String password;

        public String getUserName() {
          return userName;
        }

        public String getPassword() {
          return password;
        }
      }

      @JsonProperty("host_list")
      private String hosts;
      @JsonProperty("replication_factor")
      private int replicationFactor;
      @JsonProperty("write_delay")
      private int writeDelay;
      @JsonProperty("write_buffer_max_size")
      private int writeBufferMaxSize;
      @JsonProperty("single_row_read_size")
      private int singleRowReadSize;
      @JsonProperty("multi_row_size")
      private int multiRowSize;
      @JsonProperty("multi_row_read_size")
      private int multiRowReadSize;
      @JsonProperty("increase_buffer_size_schedule")
      private String increaseBufferSizeSchedule;
      private Auth auth = new Auth();

      public Auth getAuth() {
        return auth;
      }

      @JsonProperty("host_list")
      public String getHosts() {
        return hosts;
      }

      public int getReplicationFactor() {
        return replicationFactor;
      }

      public int getWriteDelay() {
        return writeDelay;
      }

      public int getWriteBufferMaxSize() {
        return writeBufferMaxSize;
      }

      public int getSingleRowReadSize() {
        return singleRowReadSize;
      }

      public int getMultiRowSize() {
        return multiRowSize;
      }

      public int getMultiRowReadSize() {
        return multiRowReadSize;
      }

      public String getIncreaseBufferSizeSchedule() {
        return increaseBufferSizeSchedule;
      }
    }

    public static class HBase {
      @JsonProperty("timeseries_table")
      private String timeSeriesTable;
      @JsonProperty("uinqueids_table")
      private String uinqueidsTable;
      @JsonProperty("zoo_keeper_quorum")
      private String zooKeeperQuorum;
      @JsonProperty("zoo_keeper_base_dir")
      private String zooKeeperBaseDir;
      @JsonProperty("auto_create_metrics")
      private boolean autoCreateMetrics;

      public String gettimeseriesTable() {
        return timeSeriesTable;
      }

      public String getuinqueidsTable() {
        return uinqueidsTable;
      }

      public String getzooKeeperQuorum() {
        return zooKeeperQuorum;
      }

      public String getzooKeeperBaseDir() {
        return zooKeeperBaseDir;
      }

      public boolean getautoCreateMetrics() {
        return autoCreateMetrics;
      }
    }

    public static class Remote {
      @JsonProperty("data_dir")
      private String dataDir;
      @JsonProperty("remote_url")
      private String remoteUrl;
      @JsonProperty
      private String schedule;

      public String getDataDir() {
        return dataDir;
      }

      public String getRemoteUrl() {
        return remoteUrl;
      }

      public String getSchedule() {
        return schedule;
      }
    }

    @JsonProperty
    private int concurrentQueryThreads;
    @JsonProperty
    private H2 h2 = new H2();
    @JsonProperty
    private Cassandra cassandra = new Cassandra();
    @JsonProperty
    private HBase hbase = new HBase();
    @JsonProperty
    private Remote remote = new Remote();
  
    public int getConcurrentQueryThreads() {
      return concurrentQueryThreads;
    }

    public HBase getHBase() {
      return hbase;
    }

    public H2 getH2() {
      return h2;
    }

    public Cassandra getCassandra() {
      return cassandra;
    }

    public Remote getRemote() {
      return remote;
    }
  }

  @JsonProperty
  private String hostname;
  @JsonProperty("telnetserver")
  private TelnetServer telnetServer = new TelnetServer();
  @JsonProperty
  private Reporter reporter = new Reporter();
  @JsonProperty
  private Service service = new Service();
  @JsonProperty("query_cache")
  private QueryCache queryCache = new QueryCache();
  @JsonProperty
  private Datastore datastore = new Datastore();

  public String getHostName() {
    return hostname;
  }

  @JsonProperty("telnetserver")
  public TelnetServer getTelnetServer() {
    return telnetServer;
  }

  public Reporter getReporter() {
    return reporter;
  }

  public Service getService() {
    return service;
  }

  public QueryCache getQueryCache() {
    return queryCache;
  }

  public Datastore getDatastore() {
    return datastore;
  }
}
