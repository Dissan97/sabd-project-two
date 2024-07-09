package dissanuddinahmed.queries.functions.second;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class GroupFailures implements AllWindowFunction<Tuple4<Long, Integer, Long, String>,
        Tuple4<Long, Integer, Long, String>, TimeWindow> {

    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple4<Long, Integer, Long, String>> input,
                      Collector<Tuple4<Long, Integer, Long, String>> out) {


        PriorityQueue<Tuple4<Long, Integer, Long, String>> queue = new PriorityQueue<>(
                Comparator.comparingLong(tuple -> tuple.f2)
        );

        for (Tuple4<Long, Integer, Long, String> failureCount: input){
            queue.offer(failureCount);
            if (queue.size() > 10){
                queue.poll();
            }
        }

        int size = queue.size();
        List<Tuple4<Long, Integer, Long, String>> temp = new ArrayList<>();
        while (!queue.isEmpty()){
            temp.add(queue.poll());
        }
        for (int i = size-1; i >= 0; i--){
            out.collect(temp.get(i));
        }


    }

}