package org.apache.calcite.adapter.rdf;

import java.io.File;
import java.util.Map;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

/**
 * Factory that creates a {@link RdfSchema} from a triples file.
 */
public class RdfSchemaFactory implements SchemaFactory {

  /**
   * Public singleton, per factory contract.
   */
  public static final RdfSchemaFactory INSTANCE = new RdfSchemaFactory();

  private RdfSchemaFactory() {
    // Private constructor.
  }

  @Override
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    final String triplesFilePath = (String) operand.get("triples_file");
    final File base = (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
    File triplesFile = new File(triplesFilePath);

    // Correctly read from the model base folder.
    if (base != null && !triplesFile.isAbsolute()) {
      triplesFile = new File(base, triplesFilePath);
    }
    return new RdfSchema(triplesFile);
  }
}
