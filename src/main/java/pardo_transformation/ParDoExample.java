package pardo_transformation;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

class CustFilter extends DoFn<String,String>{
    @ProcessElement
    public void processElement(ProcessContext c){
        String line = c.element();
        String[] arr = line.split(",");
        if(arr[3].equals("Delhi")) {
            c.output(line);
        }
    }
}

public class ParDoExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> pCollection = pipeline.apply(TextIO.read().from("src/main/resources/pardo/pardo_cust.csv"));
        //using ParDo
        PCollection<String> pOutput = pCollection.apply(ParDo.of(new CustFilter()));
        pOutput.apply(TextIO.write().to("src/main/resources/pardo/pardo_cust_out.csv").withHeader("id , name , lastName , city").withNumShards(1).withSuffix(".csv"));
        pipeline.run();
    }
}

