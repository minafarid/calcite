package org.apache.calcite.adapter.rdf;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.Ignore;
import org.junit.Test;

public class RdfTest {

  /**
   * Tests the vanity driver.
   */
  @Ignore
  @Test
  public void testVanityDriver() throws SQLException {
    Properties info = new Properties();
    Connection connection = DriverManager.getConnection("jdbc:rdf:", info);
    connection.close();
  }
}
