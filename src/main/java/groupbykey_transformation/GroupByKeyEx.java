package groupbykey_transformation;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
class StringToKV extends DoFn<String,KV<String,Integer>>{
    @ProcessElement
    public void processElement(ProcessContext c){
        String input = c.element();
        String arr[] = input.split(",");
        c.output(KV.of(arr[0],Integer.valueOf(arr[3])));
    }
}
class KVToString extends DoFn<KV<String,Iterable<Integer>>,String>{
    @ProcessElement
    public void processElement(ProcessContext c){
        String strKey = c.element().getKey();
        Iterable<Integer> values = c.element().getValue();
        Integer sum =0;
        for (Integer integer:values){
            sum+=integer;
        }
        c.output(strKey+","+sum.toString());
    }
}
public class GroupByKeyEx {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> pCollection = pipeline.apply(TextIO.read().from("src/main/resources/groupbykey/GroupByKey_data.csv"));
        // convert String to Key Value Pair
        PCollection<KV<String,Integer>> pOutput = pCollection.apply(ParDo.of(new StringToKV()));
        //Applying Group By key
        PCollection<KV<String,Iterable<Integer>>> kvpCollection = pOutput.apply(GroupByKey.<String,Integer>create());
        //converting kvpCollection value in string
        PCollection<String> output = kvpCollection.apply(ParDo.of(new KVToString()));
        output.apply(TextIO.write().to("src/main/resources/groupbykey/GroupByKey_out_data.csv").withHeader("cId,amount").withNumShards(1).withSuffix(".csv"));
        pipeline.run();
    }
}

