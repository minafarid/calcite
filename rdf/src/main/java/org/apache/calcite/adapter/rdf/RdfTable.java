package org.apache.calcite.adapter.rdf;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Source;
import org.apache.jena.arq.querybuilder.SelectBuilder;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;

/**
 * Base class for a table that reads data from RDF type triples.
 */
public class RdfTable extends AbstractTable implements ScannableTable {

  private RelProtoDataType protoRowType;
  private final RdfSchema rdfSchema;
  private final Schema.TableType rdfTableType;
  private final String typeUri;

  /**
   * All the properties that are available for the RDF Type that is represented by this table. This object maps the property local name -> full URI.
   */
  private final Map<String, String> propertiesMap;

  /**
   * A list of all properties that are available for this type. Constructed from the keys of {@link RdfTable#propertiesMap}.
   */
  private final List<String> allProperties;

  protected final Source source;
  protected final Model model;

  RdfTable(RdfSchema rdfSchema, Source source, Model model, String typeUri, RelProtoDataType protoRowType) {
    super();
    this.rdfSchema = rdfSchema;
    this.source = source;
    this.model = model;
    this.typeUri = typeUri;
    this.rdfTableType = TableType.VIEW;

    // Get a list of properties that appear with this type.
    this.propertiesMap = new HashMap<>();
    String typePropertiesQueryString =
        "SELECT DISTINCT ?prop "
            + "WHERE { ?a <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + typeUri + "> . "
            + "        ?a ?prop ?v }";
    Query query = QueryFactory.create(typePropertiesQueryString);
    try (QueryExecution qexec = QueryExecutionFactory.create(query, model)) {
      ResultSet results = qexec.execSelect();
      while (results.hasNext()) {
        QuerySolution soln = results.nextSolution();
        Resource property = soln.getResource("prop");
        propertiesMap.put(property.getLocalName(), property.getURI());
      }
    }

    // Get a list of all properties.
    allProperties = new ArrayList<>(propertiesMap.keySet());
  }

  @Override
  public String toString() {
    return "RdfTable for type: " + typeUri;
  }

  @Override
  public Enumerable<Object[]> scan(DataContext root) {
    return new AbstractEnumerable<Object[]>() {
      @Override
      public Enumerator<Object[]> enumerator() {
        SelectBuilder sb = new SelectBuilder()
            .addVar("*")
            .addWhere("?s", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "<" + typeUri + ">");

        for (String property : allProperties) {
          String propertyUri = propertiesMap.get(property);
          if (property.equals("type")) {
            continue;
          }
          sb.addWhere("?s", NodeFactory.createURI(propertyUri), "?" + property);
        }

        Query query = sb.build();
        System.err.println(query.toString(Syntax.syntaxSPARQL));

        return new RdfEnumerator(model, query, allProperties);
      }
    };
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    JavaTypeFactory javaTypeFactory = (JavaTypeFactory) typeFactory;
    List<RelDataType> stringTypes = allProperties
        .stream()
        .map(p -> javaTypeFactory.createSqlType(SqlTypeName.VARCHAR))
        .collect(Collectors.toList());
    return javaTypeFactory.createStructType(Pair.zip(allProperties, stringTypes));
  }
}
