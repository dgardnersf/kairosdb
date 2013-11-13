package org.kairosdb.core;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.hubspot.dropwizard.guice.DropwizardEnvironmentModule;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import org.kairosdb.core.cli.ImportCommand;
import org.kairosdb.core.cli.ExportCommand;
import org.kairosdb.core.CoreModule;
import org.kairosdb.core.http.MonitorFilter;
import org.kairosdb.core.http.rest.MetricsResource;
import org.kairosdb.core.reporting.MetricReportingModule;
import org.kairosdb.core.telnet.TelnetServerModule;
import org.kairosdb.core.scheduler.KairosDBScheduler;
import org.kairosdb.core.telnet.TelnetServer;
import org.kairosdb.datastore.cassandra.CassandraModule;

public class KairosDBApplication extends Application<KairosDBConfiguration> {
  private Injector injector;
  private DropwizardEnvironmentModule<KairosDBConfiguration> dropwizardEnvironmentModule;

  public static void main(String[] args) throws Exception {
		new KairosDBApplication().run(args);
	}

	@Override
	public void initialize(Bootstrap<KairosDBConfiguration> bootstrap) {
    dropwizardEnvironmentModule = 
      new DropwizardEnvironmentModule<KairosDBConfiguration>(KairosDBConfiguration.class);
    injector = Guice.createInjector(dropwizardEnvironmentModule, new KairosDBModule(),
                                                                 new CoreModule(), 
                                                                 new TelnetServerModule(), 
                                                                 new CassandraModule() );

    bootstrap.addCommand(new ImportCommand(this, injector, dropwizardEnvironmentModule));
    bootstrap.addCommand(new ExportCommand(this, injector, dropwizardEnvironmentModule));
	}

  @Override
  public String getName() {
    return "kairosdb";
  }

  @Override
  public void run(KairosDBConfiguration configuration, Environment environment) throws Exception {
    dropwizardEnvironmentModule.setEnvironmentData(configuration, environment);

    environment.jersey().register(injector.getInstance(MetricsResource.class));
    environment.servlets().addFilter("monitor", injector.getInstance(MonitorFilter.class));
    
    environment.lifecycle().manage(injector.getInstance(KairosDBScheduler.class));
    environment.lifecycle().manage(injector.getInstance(TelnetServer.class));
  }
}
