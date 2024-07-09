package dissanuddinahmed.queries.functions.first;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class FilterVault extends RichFilterFunction<Tuple3<Long, Integer, Double>> {
  @Override
  public boolean filter(Tuple3<Long, Integer, Double> longIntegerDoubleTuple3) throws Exception {
    return false;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
  }
}
