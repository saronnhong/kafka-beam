// import org.apache.beam.sdk.Pipeline
// import org.apache.beam.sdk.options.PipelineOptionsFactory
// import org.apache.beam.sdk.transforms.{DoFn, ParDo}
// import org.apache.beam.sdk.transforms.DoFn.ProcessElement
// import org.apache.beam.sdk.values.PCollection
// import scala.jdk.CollectionConverters._

// object Sample {
//   def main(args: Array[String]): Unit = {
//     // Define the pipeline options
//     val options = PipelineOptionsFactory.create()

//     // Create the pipeline
//     val pipeline = Pipeline.create(options)

//     // Input data (normally from a file or other source)
//     val inputData = Seq("hello world", "apache beam", "scala integration").asJava

//     // Create a PCollection from the input data
//     val inputCollection: PCollection[String] =
//       pipeline.apply("CreateInput", org.apache.beam.sdk.transforms.Create.of(inputData))

//     // Apply a transformation to convert strings to uppercase
//     val uppercaseCollection: PCollection[String] = inputCollection.apply(
//       "ConvertToUppercase",
//       ParDo.of(new UppercaseDoFn)
//     )

//     // Apply another transformation to print the output to the console
//     uppercaseCollection.apply(
//       "PrintToConsole",
//       ParDo.of(new PrintDoFn)
//     )

//     // Run the pipeline
//     pipeline.run().waitUntilFinish()
//   }

//   // Define a DoFn for converting strings to uppercase
//   class UppercaseDoFn extends DoFn[String, String] {
//     @ProcessElement
//     def processElement(context: DoFn[String, String]#ProcessContext): Unit = {
//       val input = context.element()
//       context.output(input.toUpperCase)
//     }
//   }

//   // Define a DoFn for printing elements to the console
//   class PrintDoFn extends DoFn[String, Void] {
//     @ProcessElement
//     def processElement(context: DoFn[String, Void]#ProcessContext): Unit = {
//       println(context.element())
//     }
//   }
// }
