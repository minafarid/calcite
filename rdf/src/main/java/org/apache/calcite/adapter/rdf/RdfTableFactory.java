package org.apache.calcite.adapter.rdf;

import java.io.File;
import java.util.Map;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;

/**
 * Factory that creates an {@link RdfTable}.
 */
public class RdfTableFactory implements TableFactory<RdfTable> {

  /**
   * Public constructor, as per factory contract.
   */
  public RdfTableFactory() {

  }

  @Override
  public RdfTable create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {
    String typeUri = (String) operand.get("type_uri");
    String triplesFile = (String) operand.get("triples_file");
    final File base = (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
    final Source source = Sources.file(base, triplesFile);
    final RelProtoDataType protoRowType = rowType != null ? RelDataTypeImpl.proto(rowType) : null;
    Model model = RDFDataMgr.loadModel(source.file().getAbsolutePath());

    return new RdfTable(null, source, model, typeUri, protoRowType);
  }
}
