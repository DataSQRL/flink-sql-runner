package com.datasqrl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.net.URISyntaxException;
import org.apache.flink.shaded.guava31.com.google.common.io.Resources;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import picocli.CommandLine;

@ExtendWith(MiniClusterExtension.class)
class SqlRunnerTest {

  private File sqlFile;
  private File sqlUdfFile;
  private File planFile;
  private File configDir;
  private String udfPath;

  @BeforeEach
  void setUp() throws URISyntaxException {
    sqlFile = new File(Resources.getResource("sql/test_sql.sql").toURI());
    sqlUdfFile = new File(Resources.getResource("sql/test_udf_sql.sql").toURI());
    planFile = new File(Resources.getResource("plans/test_plan.json").toURI());
    configDir = new File(Resources.getResource("config").toURI());

    // Set UDF path to the 'udfs' directory in resources
    udfPath = new File(Resources.getResource("udfs").toURI()).getAbsolutePath();
  }

  @Test
  void testCommandLineInvocationWithSqlFile() {
    // Simulating passing command-line arguments using picocli
    String[] args = {"-s", sqlFile.getAbsolutePath(), "--block"};

    // Use CommandLine to parse and execute
    CommandLine cmd = new CommandLine(new SqlRunner());
    int exitCode = cmd.execute(args); // Executes the SqlRunner logic with arguments

    // Assert the exit code is as expected (0 for success)
    assertEquals(0, exitCode);
  }

  @Test
  void testCompiledPlan() {
    String[] args = {
      "--configfile", configDir.getAbsolutePath(),
      "--planfile", planFile.getAbsolutePath(),
      "--block"
    };

    CommandLine cmd = new CommandLine(new SqlRunner());
    int exitCode = cmd.execute(args);

    // Assert failure exit code (1 for failure due to invalid argument combination)
    assertEquals(0, exitCode);
  }

  @Test
  void testUdf() {
    String[] args = {
      "--configfile",
      configDir.getAbsolutePath(),
      "-s",
      sqlUdfFile.getAbsolutePath(),
      "--udfpath",
      udfPath,
      "--block"
    };

    CommandLine cmd = new CommandLine(new SqlRunner());
    int exitCode = cmd.execute(args);

    // Assert failure exit code (1 for failure due to invalid argument combination)
    assertEquals(0, exitCode);
  }
}
