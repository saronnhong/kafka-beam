// import org.apache.beam.sdk.Pipeline
// import org.apache.beam.sdk.io.TextIO
// import org.apache.beam.sdk.transforms.{DoFn, ParDo}
// import org.apache.beam.sdk.values.PCollection
// import org.apache.beam.sdk.PipelineOptionsFactory
// import org.apache.beam.sdk.transforms.DoFn.ProcessElement
// import scala.jdk.CollectionConverters._

// object Main {
//   def main(args: Array[String]): Unit = {
//     // Define the pipeline options
//     val options = PipelineOptionsFactory.create()

//     // Create the pipeline
//     val pipeline = Pipeline.create(options)

//     // Step 1: Read CSV lines from an input file (e.g., input.csv)
//     val inputFilePath = getClass.getResource("/input.csv").getPath
//     val inputCollection: PCollection[String] =
//       pipeline.apply("ReadCSV", TextIO.read().from(inputFilePath))

//     // Step 2: Process each line (for now, just print it as if it were streaming)
//     inputCollection.apply("ProcessData", ParDo.of(new ProcessCSVLine))

//     // Step 3: Write the results to an output file (e.g., output.txt)
//     val outputFilePath = "output.txt"
//     inputCollection.apply("WriteResults", TextIO.write().to(outputFilePath))

//     // Run the pipeline
//     pipeline.run().waitUntilFinish()
//   }

//   // A simple DoFn to process each line (for now just printing it)
//   class ProcessCSVLine extends DoFn[String, String] {
//     @ProcessElement
//     def processElement(context: DoFn[String, String]#ProcessContext): Unit = {
//       val line = context.element()
//       // For now, just print the line as is
//       println(line)
//       context.output(line)
//     }
//   }
// }
