package sample;

import org.apache.flink.table.functions.ScalarFunction;

import com.datasqrl.function.AutoRegisterSystemFunction;
import com.google.auto.service.AutoService;

/**
 * Converts the input string to uppercase.
 * If the input string is null, a null value is returned.
 */
@AutoService(AutoRegisterSystemFunction.class)
public class Upper extends ScalarFunction implements AutoRegisterSystemFunction {

  public String eval(String text) {
    if (text == null) {
      return null;
    }

    return text.toUpperCase();
  }

}