package org.kairosdb.core.cli;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Optional;
import com.google.inject.Injector;
import com.hubspot.dropwizard.guice.DropwizardEnvironmentModule;
import io.dropwizard.Application;
import io.dropwizard.cli.EnvironmentCommand;
import io.dropwizard.setup.Environment;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.Writer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.kairosdb.core.KairosDBConfiguration;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryCallback;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.exception.DatastoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExportCommand extends EnvironmentCommand<KairosDBConfiguration> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExportCommand.class);

    private Injector injector;
    private DropwizardEnvironmentModule dropwizardEnvironmentModule;

    public ExportCommand(Application application,
        Injector injector, DropwizardEnvironmentModule dropwizardEnvironmentModule) {
        super(application, "export", "Export datapoints");
        this.injector = injector;
        this.dropwizardEnvironmentModule = dropwizardEnvironmentModule;
    }

    @Override
    public void configure(Subparser subparser) {
        super.configure(subparser);
        subparser.addArgument("-f", "--export-file")
          .help("file to save export to or read from depending on command");
        subparser.addArgument("-n", "--names")
                 .help("comma seperated list of names of metrics to export. " + 
                     "If not specified, then all metrics are exported");
    }

    @Override
    protected void run(Environment environment, Namespace namespace, KairosDBConfiguration configuration) 
      throws Exception {
      dropwizardEnvironmentModule.setEnvironmentData(configuration, environment);

      String names = (String) namespace.get("names");
      List<String> namesList = null;
      if (namespace.get("names") != null) {
        namesList = Arrays.asList(names.split(","));
      } else {
        namesList = Collections.EMPTY_LIST;
      }

      String exportFileName = (String) namespace.get("export_file");
      OutputStream out = System.out;
      if (exportFileName != null) {
        out = new FileOutputStream(exportFileName);
      }
      runExport(new OutputStreamWriter(out, "UTF-8"), namesList);
      System.out.flush();
      System.exit(0);
    }

    private void runExport(Writer out, List<String> metricNames) throws DatastoreException, IOException
    {
      KairosDatastore ds = injector.getInstance(KairosDatastore.class);
      Iterable<String> metrics;

      if (metricNames != null && metricNames.size() > 0)
        metrics = metricNames;
      else
        metrics = ds.getMetricNames();

      for (String metric : metrics)
      {
        LOGGER.info("Exporting: " + metric);
        QueryMetric qm = new QueryMetric(1L, 0, metric);
        ExportQueryCallback callback = new ExportQueryCallback(metric, out);
        ds.export(qm, callback);
      }

      out.flush();
    }


    private static class ExportQueryCallback implements QueryCallback
    {
      private final Writer m_writer;
      private JsonGenerator m_jsonWriter;
      private final String m_metric;

      public ExportQueryCallback(String metricName, Writer out)
      {
        m_metric = metricName;
        m_writer = out;
      }

      @Override
      public void addDataPoint(long timestamp, long value) throws IOException
      {
        try
        {
          m_jsonWriter.writeStartArray();
          m_jsonWriter.writeNumber(timestamp);
          m_jsonWriter.writeNumber(value);
          m_jsonWriter.writeEndArray();
        }
        catch (JsonGenerationException e)
        {
          throw new IOException(e);
        }
      }

      @Override
      public void addDataPoint(long timestamp, double value) throws IOException
      {
        try
        {
          m_jsonWriter.writeStartArray();
          m_jsonWriter.writeNumber(timestamp);
          m_jsonWriter.writeNumber(value);
          m_jsonWriter.writeEndArray();
        }
        catch (JsonGenerationException e)
        {
          throw new IOException(e);
        }
      }

      @Override
      public void startDataPointSet(Map<String, String> tags) throws IOException
      {
        if (m_jsonWriter != null)
          endDataPoints();

        try
        {
          m_jsonWriter = new JsonFactory().createGenerator(m_writer);
          m_jsonWriter.writeStartObject();
          m_jsonWriter.writeFieldName("name");
          m_jsonWriter.writeString(m_metric);
          m_jsonWriter.writeFieldName("tags");

          m_jsonWriter.writeStartObject();
          for (Map.Entry<String, String> entry : tags.entrySet()) {
            m_jsonWriter.writeFieldName(entry.getKey());
            m_jsonWriter.writeString(entry.getValue());
          }
          m_jsonWriter.writeEndObject();

          m_jsonWriter.writeFieldName("datapoints");
          m_jsonWriter.writeStartArray();

        }
        catch (JsonGenerationException e)
        {
          throw new IOException(e);
        }
      }

      @Override
      public void endDataPoints() throws IOException
      {
        try
        {
          if (m_jsonWriter != null)
          {
            m_jsonWriter.writeEndArray();
            m_jsonWriter.writeEndObject();
            m_jsonWriter.flush();
            m_writer.write("\n");
            m_jsonWriter = null;
          }
        }
        catch (JsonGenerationException e)
        {
          throw new IOException(e);
        }

      }
    }
}

