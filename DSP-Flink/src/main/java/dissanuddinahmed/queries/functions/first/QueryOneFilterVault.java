package dissanuddinahmed.queries.functions.first;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class QueryOneFilterVault implements FilterFunction<Tuple3<Long, Integer, Double>> {


    @Override
    public boolean filter(Tuple3<Long, Integer, Double> tuple3) {
        return tuple3.f1 >= 1000 && tuple3.f1 <= 1020;
    }


}
