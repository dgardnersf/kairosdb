KairosDB is a rewrite of the original OpenTSDB (http://opentsdb.net) project.

Documentation is found [here](http://code.google.com/p/kairosdb/).

CHANGES from KairosDB:
---------------------

Wed Nov 13 08:24:23 PST 2013

- added maven build
- replace properties file with dropwizard YAML configuration
- use Jackson JsonGenerator instead of org.json JSONWriter
- update older codehaus Jackson to use dropwizard 0.7.0 version of Jackson (2.2.3)
- use Hibernate validator instead of Apache validator
- replace http/servlet WebServer, WebServletModule etc with dropwizard framework
- replaced kairos Main with dropwizard framework, Managed objects, and Command
  objects
- dropped h2, remote, hbase datastores for now - add back later as dropwizard 
  bundles
- dropped rpm packaging - add back later as maven task
