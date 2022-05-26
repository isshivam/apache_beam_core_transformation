package flatten_transformation;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;


public class FlattenExample {
    public static void main(String[] args) {
        Pipeline pipeline =Pipeline.create();
        PCollection<String> pCustList1 = pipeline.apply(TextIO.read().from("src/main/resources/flatten/customer_1.csv"));
        PCollection<String> pCustList2 = pipeline.apply(TextIO.read().from("src/main/resources/flatten/customer_2.csv"));
        PCollection<String> pCustList3 = pipeline.apply(TextIO.read().from("src/main/resources/flatten/customer_3.csv"));
        PCollectionList<String> list = PCollectionList.of(pCustList1).and(pCustList2).and(pCustList3);
        PCollection<String> merged =list.apply(Flatten.pCollections());
        merged.apply(TextIO.write().to("src/main/resources/flatten/flatten_customer_output.csv").withNumShards(1).withSuffix(".csv"));
        pipeline.run();
    }
}

