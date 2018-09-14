package org.apache.calcite.adapter.rdf;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Source;

/**
 * Base class for a table that reads data from RDF type triples.
 */
public class RdfTable extends AbstractTable {
  protected final Source source;
  protected final RelProtoDataType protoRowType;

  RdfTable(Source source, RelProtoDataType protoRowType) {
    this.source = source;
    this.protoRowType = protoRowType;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return null;
  }
}
