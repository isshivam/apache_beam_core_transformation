package combine_transformation;

import org.apache.beam.sdk.transforms.SerializableFunction;

import java.util.ArrayList;
import java.util.List;

class CombineExample implements SerializableFunction<Iterable<Integer>, Integer> {
    @Override
    public Integer apply(Iterable<Integer> input) {
        int sum = 0;
        for (int item : input) {
            sum += item;
        }
        return sum;
    }
}
public class SumInts {
    public static void main(String[] args) {
        List<Integer> list = new ArrayList<>();
        list.add(10);
        list.add(20);
        list.add(30);
        CombineExample combineExample = new CombineExample();
        int result = combineExample.apply(list);
        System.out.println(result);
    }
}