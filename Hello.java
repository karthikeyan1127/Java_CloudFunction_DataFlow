import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.LaunchTemplateParameters;
import com.google.api.services.dataflow.model.RuntimeEnvironment;
import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedWriter;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public class Hello implements HttpFunction {
    @Override
    public void service(HttpRequest request, HttpResponse response) throws Exception {
// Authentication is provided by gcloud tool when running locally
        // and by built-in service accounts when running on GAE, GCE or GKE.
        Logger Log = LoggerFactory.getLogger(this.getClass());
        BufferedWriter writer = response.getWriter();
        try {
            Log.info("!");
            writer.write("1");
            HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
            JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
            GoogleCredential credential = GoogleCredential.getApplicationDefault(httpTransport, jsonFactory);
            writer.write("2");
            Log.info("!");
            // The createScopedRequired method returns true when running on GAE or a local developer
            // machine. In that case, the desired scopes must be passed in manually. When the code is
            // running in GCE, GKE or a Managed VM, the scopes are pulled from the GCE metadata server.
            // See https://developers.google.com/identity/protocols/application-default-credentials for more information.
            if (credential.createScopedRequired()) {
                credential = credential.createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
            }
            Log.info("qweweeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
            writer.write("3");
            Dataflow dataflowService = new Dataflow.Builder(httpTransport, jsonFactory, credential)
                    .setApplicationName("Google Cloud Platform Sample")
                    .build();

            // Add your code to assign values to parameters for the 'create' method:
            // * The project which owns the job.
            String projectId = "tidal-pathway-285009";

            RuntimeEnvironment runtimeEnvironment = new RuntimeEnvironment();
            runtimeEnvironment.setBypassTempDirValidation(false);
            runtimeEnvironment.setTempLocation("gs://karthiksfirstbucket/temp1");

            LaunchTemplateParameters launchTemplateParameters = new LaunchTemplateParameters();
            launchTemplateParameters.setEnvironment(runtimeEnvironment);
            launchTemplateParameters.setJobName("newJob" + (new Date()).getTime());
            Map<String, String> params = new HashMap<String, String>();
            params.put("inputFile", "gs://karthiksfirstbucket/sample");
            params.put("outputFile", "gs://karthiksfirstbucket/count1");
            params.put("gcsPath","https://storage.cloud.google.com/pipeline-staging-test/templates/WordCount");
            launchTemplateParameters.setParameters(params);
            writer.write("4");
            Log.info("5646564564645");
            Dataflow.Projects.Templates.Launch launch = dataflowService.projects().templates().launch(projectId, launchTemplateParameters);
            Log.info("56next");
            launch.setGcsPath("https://storage.cloud.google.com/pipeline-staging-test/templates/WordCount");
            launch.execute();
            Log.info("&&&&&&&&&&&&&&&&");
        }catch(Exception e){
            writer.write(e.toString());
        }
        /*HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
        GoogleCredentials.getApplicationDefault();
        Dataflow dataflowService = new Dataflow.Builder(httpTransport, jsonFactory, null)
                .setApplicationName("Google Cloud Platform Sample")
                .build();


        String projectId = "tidal-pathway-285009";

        RuntimeEnvironment runtimeEnvironment = new RuntimeEnvironment();
        runtimeEnvironment.setBypassTempDirValidation(false);
        runtimeEnvironment.setTempLocation("gs://karthiksfirstbucket/temp1");
        runtimeEnvironment.setIpConfiguration("WORKER_IP_UNSPECIFIED");
        runtimeEnvironment.setAdditionalExperiments(new ArrayList());

        CreateJobFromTemplateRequest createJobFromTemplateRequest = new CreateJobFromTemplateRequest();
        createJobFromTemplateRequest.setEnvironment(runtimeEnvironment);
        createJobFromTemplateRequest.setLocation("us-central1");
        createJobFromTemplateRequest.setGcsPath("gs://pipeline-staging-test/templates/WordCount");
        createJobFromTemplateRequest.setJobName("karthikssecondjob"+(new Date()).getTime());
        createJobFromTemplateRequest.setParameters(new HashMap<String,String>());
        createJobFromTemplateRequest.getParameters().put("inputFile","gs://karthiksfirstbucket/sample.txt");
        createJobFromTemplateRequest.getParameters().put( "output", "gs://karthiksfirstbucket/count1");
        Dataflow.Projects.Templates.Create result = dataflowService.projects().templates().create(projectId,createJobFromTemplateRequest);


        System.out.println(result.getProjectId());
    }

    public static void main() throws Exception {
        String args[] = {"--project=tidal-pathway-285009","--stagingLocation=gs://karthiksfirstbucket/staging/","--output=gs://karthiksfirstbucket/count1","--tempLocation=gs://karthiksfirstbucket/temp1/","--runner=DataflowRunner","--region=us-central1"};
        Pipeline p = Pipeline.create(
                PipelineOptionsFactory.fromArgs(args).withValidation().create());
        p.apply(Create.of("Hello", "World"))
                .apply(MapElements.via(new SimpleFunction<String, String>() {
                    @Override
                    public String apply(String input) {
                        return input.toUpperCase();
                    }
                }))
                .apply(ParDo.of(new DoFn<String, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c)  {
                    }
                }));

        p.run();
    }
    }*/
    }
    public static void main(String args[]) throws Exception {
        Hello hello = new Hello();
        hello.service(null,null);
    }
}
