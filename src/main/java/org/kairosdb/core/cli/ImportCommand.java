package org.kairosdb.core.cli;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.google.common.base.Optional;
import com.google.inject.Injector;
import com.hubspot.dropwizard.guice.DropwizardEnvironmentModule;
import io.dropwizard.Application;
import io.dropwizard.cli.EnvironmentCommand;
import io.dropwizard.setup.Environment;
import java.io.BufferedReader;
import java.io.StringReader;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.kairosdb.core.KairosDBConfiguration;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.http.rest.json.JsonMetricParser;
import org.kairosdb.util.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportCommand extends EnvironmentCommand<KairosDBConfiguration> {
    public static final Charset UTF_8 = Charset.forName("UTF-8");
    private static final Logger LOGGER = LoggerFactory.getLogger(ImportCommand.class);

    private Injector injector;
    private DropwizardEnvironmentModule dropwizardEnvironmentModule;

    public ImportCommand(Application application,
        Injector injector, DropwizardEnvironmentModule dropwizardEnvironmentModule) {
        super(application, "import", "Import datapoints");
        this.injector = injector;
        this.dropwizardEnvironmentModule = dropwizardEnvironmentModule;
    }

    @Override
    public void configure(Subparser subparser) {
        super.configure(subparser);
        subparser.addArgument("-f", "--import-file")
          .help("file to save export to or read from depending on command");
    }

    @Override
    protected void run(Environment environment, Namespace namespace, KairosDBConfiguration configuration) 
      throws Exception {
      dropwizardEnvironmentModule.setEnvironmentData(configuration, environment);
      String importFileName = (String) namespace.get("import_file");
      InputStream in = System.in;
      LOGGER.info("importing data from " + importFileName != null ? importFileName : "stdin");
      if (importFileName != null) {
        in = new FileInputStream(importFileName);
      }
      runImport(in);
      System.exit(0);
    }

    public void runImport(InputStream in) throws IOException,
      InterruptedException, JsonGenerationException, DatastoreException, ValidationException {
      KairosDatastore ds = injector.getInstance(KairosDatastore.class);

      BufferedReader reader = new BufferedReader(new InputStreamReader(in, UTF_8));

      String line = null;
      while ((line = reader.readLine()) != null)
      {
        JsonMetricParser jsonMetricParser = new JsonMetricParser(ds, new StringReader(line));

        jsonMetricParser.parse();
      }
      ds.close();
    }
}
