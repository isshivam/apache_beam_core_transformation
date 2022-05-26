package cogroupbykey_transformation;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

class OrderParsing extends DoFn<String,KV<String,String>>{
    @ProcessElement
    public void processElement(ProcessContext c){
        String arr[] = c.element().split(",");
        String strKey=arr[0];
        String strVal =arr[1]+","+arr[2]+","+arr[3];
        c.output(KV.of(strKey,strVal));
    }
}
class UserParsing extends DoFn<String,KV<String,String>>{
    @ProcessElement
    public void processElement(ProcessContext c){
        String arr[] = c.element().split(",");
        String strKey=arr[0];
        String strVal =arr[1];
        c.output(KV.of(strKey,strVal));
    }
}
public class CoGroupByEx {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        // Converting  String to KV Object
        PCollection<KV<String,String>> pOrderCollection = pipeline.apply(TextIO.read().from("src/main/resources/cogroupbykey/user_order.csv")).apply(ParDo.of(new OrderParsing()));
        PCollection<KV<String,String>> pUserCollection = pipeline.apply(TextIO.read().from("src/main/resources/cogroupbykey/p_user.csv")).apply(ParDo.of(new UserParsing()));
        //Creating TupleTag Object
        final TupleTag<String> orderTuple = new TupleTag<String>();
        final TupleTag<String> userTuple = new TupleTag<String>();
        //combining datasets using CoGroupByKey
        PCollection<KV<String,CoGbkResult>> result = KeyedPCollectionTuple.of(orderTuple,pOrderCollection).and(userTuple,pUserCollection).apply(CoGroupByKey.<String>create());
        //iterating CoGbkResult and build String
        PCollection<String> output = result.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                String strKey = c.element().getKey();
                CoGbkResult valObject = c.element().getValue();
                Iterable<String> orderTable = valObject.getAll(orderTuple);
                Iterable<String> userTable = valObject.getAll(userTuple);
                for (String order:orderTable){
                    for (String user : userTable){
                        c.output(strKey+","+order+","+user);
                    }
                }
            }
        }));
        //step 5: save the result
        output.apply(TextIO.write().to("src/main/resources/cogroupbykey/CoGroupByKey.csv").withNumShards(1).withSuffix(".csv"));
        pipeline.run();
    }
}
