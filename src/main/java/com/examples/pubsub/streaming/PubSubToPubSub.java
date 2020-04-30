package com.examples.pubsub.streaming;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.api.services.storage.StorageScopes;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import org.apache.beam.examples.common.WriteOneFilePerWindow;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.HashSet;
import java.util.Set;

import org.apache.beam.sdk.transforms.ParDo;







//import static com.google.common.base.Preconditions.checkNotNull;
//import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;
//
//import com.google.auto.value.AutoValue;
//import java.util.regex.Pattern;
//import java.util.regex.PatternSyntaxException;
//import javax.annotation.Nullable;
//import org.apache.beam.sdk.Pipeline;
//import org.apache.beam.sdk.PipelineResult;
//import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
//import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
//import org.apache.beam.sdk.metrics.Counter;
//import org.apache.beam.sdk.metrics.Metrics;
//import org.apache.beam.sdk.options.Description;
//import org.apache.beam.sdk.options.PipelineOptions;
//import org.apache.beam.sdk.options.PipelineOptionsFactory;
//import org.apache.beam.sdk.options.StreamingOptions;
//import org.apache.beam.sdk.options.Validation;
//import org.apache.beam.sdk.options.ValueProvider;
//import org.apache.beam.sdk.transforms.DoFn;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;








public class PubSubToPubSub {
  /*
  * Define your own configuration options. Add your own arguments to be processed
  * by the command-line parser, and specify default values for them.
  */
  public interface Options extends PipelineOptions, StreamingOptions, DataflowPipelineOptions {
    @Description(
            "The Cloud Pub/Sub subscription to consume from. "
                    + "The name should be in the format of "
                    + "projects/<project-id>/subscriptions/<subscription-name>.")
    @Validation.Required
    ValueProvider<String> getInputSubscription();

    void setInputSubscription(ValueProvider<String> inputSubscription);

    @Description(
            "The Cloud Pub/Sub topic to publish to. "
                    + "The name should be in the format of "
                    + "projects/<project-id>/topics/<topic-name>.")
    @Validation.Required
    ValueProvider<String> getOutputTopic();

    void setOutputTopic(ValueProvider<String> outputTopic);

    @Description(
            "Filter events based on an optional attribute key. "
                    + "No filters are applied if a filterKey is not specified.")
    @Validation.Required
    ValueProvider<String> getFilterKey();

    void setFilterKey(ValueProvider<String> filterKey);

    @Description(
            "Filter attribute value to use in case a filterKey is provided. Accepts a valid Java regex"
                    + " string as a filterValue. In case a regex is provided, the complete expression"
                    + " should match in order for the message to be filtered. Partial matches (e.g."
                    + " substring) will not be filtered. A null filterValue is used by default.")
    @Validation.Required
    ValueProvider<String> getFilterValue();

    void setFilterValue(ValueProvider<String> filterValue);


    @Description("Output file's window size in number of minutes.")
    @Default.Integer(1)
    @Validation.Required
    Integer getWindowSize();
    void setWindowSize(Integer value);

//    @Description("Credential configuration")
//    CredentialsProvider getCredentialsProvider();
//    void setCredentialsProvider(CredentialsProvider credentialsProvider);
//
//    @Description("ServiceAccount")
//    String getServiceAccount();
//    void setServiceAccount(String serviceAccount);

  }

  public static void main(String[] args) throws IOException {
    // The maximum number of shards when writing output.
    int numShards = 1;
    String jsonPath ="C:\\Codes\\IntelliJ_IDEA_WORKSPACE\\DataFlowExample\\java-docs-samples\\pubsub\\streaming-analytics\\src\\main\\resources\\AllServicesKey.json";


    Options options = PipelineOptionsFactory
      .fromArgs(args)
      .withValidation()
      .as(Options.class);

    options.setStreaming(true);

    //CredentialsProvider credentialsProvider = FixedCredentialsProvider.create(ServiceAccountCredentials.fromStream(new FileInputStream(jsonPath)));
    Credentials credentials = ServiceAccountCredentials.fromStream(new FileInputStream(jsonPath));
    //credentials = ((ServiceAccountCredentials) credentials).createScoped("https://www.googleapis.com/auth/cloud-platform"); beam 2.19.0 version

    /**Beam 2.18*/
    Set scopesList = new HashSet();
    scopesList.add("https://www.googleapis.com/auth/cloud-platform");
    credentials =((ServiceAccountCredentials) credentials).createScoped(scopesList);
    ((ServiceAccountCredentials) credentials).refreshAccessToken();
    /**Beam 2.18*/

    options.setGcpCredential(credentials);
    options.setServiceAccount("dataflowservicetestaccount@pubsub-test-project-16951.iam.gserviceaccount.com");
    options.setStagingLocation("gs://project-16951-bucket1/stage");



    //scopesList.addAll(PubsubScopes.all());
    //scopesList.addAll(StorageScopes.all());


    options.setTempLocation("gs://project-16951-bucket1/temp");
//    options.setServiceAccount("dataflowservicetestaccount@pubsub-test-project-16951.iam.gserviceaccount.com");


    Pipeline pipeline = Pipeline.create(options);


    pipeline
      // 1) Read string messages from a Pub/Sub topic.
      //.apply("Read PubSub Messages", PubsubIO.readStrings().fromSubscription(options.getInputSubscription()))
        .apply("Read PubSub Messages", PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInputSubscription()))
      // 2) Group the messages into fixed-sized minute intervals.
      .apply(Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))))
      // 3) Write one file to GCS for every window of messages.
      .apply("Write Files to PubSub Topic",PubsubIO.writeMessages().to(options.getOutputTopic()));

    // Execute the pipeline and wait until it finishes running.
    pipeline.run().waitUntilFinish();



//    pipeline
//            .apply(
//                    "Read PubSub Events",
//                    PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInputSubscription()))
//            .apply(
//                    "Filter Events If Enabled",
//                    ParDo.of(
//                            ExtractAndFilterEventsFn.newBuilder()
//                                    .withFilterKey(options.getFilterKey())
//                                    .withFilterValue(options.getFilterValue())
//                                    .build()))
//            .apply("Write PubSub Events", PubsubIO.writeMessages().to(options.getOutputTopic()));
//
//    // Execute the pipeline and return the result.
//    return pipeline.run();
  }
}
