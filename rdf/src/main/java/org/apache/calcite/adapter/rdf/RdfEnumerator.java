package org.apache.calcite.adapter.rdf;

import java.util.Iterator;
import java.util.List;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;

public class RdfEnumerator implements Enumerator<Object[]> {
  private final Model model;
  private final Query query;
  private final List<String> selectedProperties;
  private QueryExecution queryExec;
  private ResultSet results = null;
  private QuerySolution currentSoln;

  public RdfEnumerator(Model model, Query query, List<String> selectedProperties) {
    this.model = model;
    this.query = query;
    this.selectedProperties = selectedProperties;
    queryExec = QueryExecutionFactory.create(query, model);
    results = queryExec.execSelect();
  }

  @Override
  public Object[] current() {
    return ConvertSolutionToArray(currentSoln);
  }

  private Object[] ConvertSolutionToArray(QuerySolution currentSoln) {
    Object[] result = new Object[selectedProperties.size()];
    Iterator<String> varNames = currentSoln.varNames();
    for (int i = 0; varNames.hasNext(); i++) {
      result[i] = currentSoln.get(varNames.next()).toString();
    }
    return result;
  }

  @Override
  public boolean moveNext() {
    if (results.hasNext()) {
      currentSoln = results.nextSolution();
      return true;
    }
    return false;
  }

  @Override
  public void reset() {
    if (queryExec.isClosed()) {
      queryExec = QueryExecutionFactory.create(query, model);
    }
    results = queryExec.execSelect();
  }

  @Override
  public void close() {
    queryExec.close();
  }
}
