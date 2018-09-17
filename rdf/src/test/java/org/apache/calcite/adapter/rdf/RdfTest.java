package org.apache.calcite.adapter.rdf;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.calcite.util.Sources;
import org.junit.Ignore;
import org.junit.Test;

public class RdfTest {

  /** Main model for RDF. */
  private URL modelJsonUrl = RdfTest.class.getResource("/model.json");

  /**
   * Tests the vanity driver.
   */
  @Test
  public void testRdf() throws SQLException {
    String sql = "SELECT * FROM PERSON";
    Properties info = new Properties();
    info.put("model", Sources.of(modelJsonUrl));
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(sql);
    while(resultSet.next()) {
      String col1 = resultSet.getString(1);
      System.out.println(col1);
    }
    statement.close();
    connection.close();
  }
}
