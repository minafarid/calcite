package org.apache.calcite.adapter.rdf;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFDataMgr;

public class RdfSchema extends AbstractSchema {

  /**
   * Corresponding the file that contains the triples.
   */
  private final File triplesFile;
  private Map<String, Table> tableMap;

  public RdfSchema(File file) {
    super();
    this.triplesFile = file;
  }

  @Override
  protected Map<String, Table> getTableMap() {
    if (tableMap == null) {
      tableMap = mineTables();
    }
    return tableMap;
  }

  /**
   * Performs the RDF data mining to retrieve all tables.
   *
   * @return Map of table name -> table reference (rdf:type).
   */
  private Map<String, Table> mineTables() {
    final Source source = Sources.of(triplesFile);
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

    // Create jena model.
    Model model = RDFDataMgr.loadModel(triplesFile.getAbsolutePath());
    String typesQueryString = "SELECT DISTINCT ?type WHERE { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type }";
    Query query = QueryFactory.create(typesQueryString) ;
    try (QueryExecution qexec = QueryExecutionFactory.create(query, model)) {
      ResultSet results = qexec.execSelect() ;
      while (results.hasNext())
      {
        QuerySolution soln = results.nextSolution() ;
        Resource type = soln.getResource("type") ;
        RdfTable typeTable = new RdfTable(source, null);
        builder.put(type.getLocalName(), typeTable);
      }
    }

    return builder.build();
  }
}
