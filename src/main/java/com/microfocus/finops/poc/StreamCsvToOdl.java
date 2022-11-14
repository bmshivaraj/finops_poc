package com.microfocus.finops.poc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactoryBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.ssl.TLS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamCsvToOdl {

    static AtomicInteger rowsIngested = new AtomicInteger(0);
    private static XAuthToken token;
    private int THREAD_POOL_COUNT = 50;
    private ExecutorService pool;
    private LinkedBlockingDeque<File> billPayloadFiles;
    private OdlConfiguration configuration;

    Logger logger = LoggerFactory.getLogger(StreamCsvToOdl.class);

    public static void main(String[] args) throws Exception {

        StreamCsvToOdl streamCsvToOdl = new StreamCsvToOdl();
        int totalRecCount = streamCsvToOdl.getTotalCsvLines();
        startTime = Instant.now();
        final int totalMessagesToReceive = totalRecCount;
        new Thread("Monitor") {
            @Override
            public void run() {
                try {
                    VerticaSSLConnection.readRowsCount(totalMessagesToReceive, Instant.now());
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
        }.start();

        streamCsvToOdl.startLoadingProcess();
    }

    static Instant startTime;

    private int getTotalCsvLines() throws IOException {
        int totalRecCount = 0;
        File csvSourceDir = new File("aws_csv");
        if (!csvSourceDir.exists() || !csvSourceDir.isDirectory()) {
            logger.error("Missing source directory 'aws_csv' of AWS CSV bills");
            System.exit(0);
        }

        for (File csvSourceFile : csvSourceDir.listFiles (
            new FileFilter() {
                @Override
                public boolean accept(File pathname) {
                    return pathname.getName().endsWith(".csv");
                }
            }))
        {
            totalRecCount += Util.lineCount(csvSourceFile);
            totalRecCount--; // Minus one for the header
        }

        return totalRecCount;
    }

    private void startLoadingProcess() throws KeyManagementException, NoSuchAlgorithmException, IOException {
        configuration = new OdlConfiguration();
        THREAD_POOL_COUNT = configuration.getConcurrencyCount();
        intiThreadPool();
        //generateReceiverPayloadFile();
        initHttpConnectionConfig();
        getXAuthToken();
        startTime = Instant.now();
        startLoadingData();
    }

    private void intiThreadPool() {
        billPayloadFiles = new LinkedBlockingDeque();
        logger.info("Thread count = " + THREAD_POOL_COUNT);
        pool = Executors.newFixedThreadPool(THREAD_POOL_COUNT);
    }

    private void intiDataset(String datasetName) throws IOException, NoSuchAlgorithmException, KeyManagementException {
        new JsonToDataset().jsonToDatasetConversionAndIssieODLDataSetConfigurationCmd(datasetName);
    }

    private void generateReceiverPayloadFile() throws IOException {
        new CsvToJson().csvToJsonConvertion2();
    }

    private void prepareJsonFileQueue() {
        File directoryPath = new File("aws_json");
        //System.out.println("reading from dir: " + directoryPath);
        for (File jsonFile : directoryPath.listFiles()) {
            //System.out.println("-------  " + jsonFile.getName() + "----------");
            billPayloadFiles.add(jsonFile);
        }
    }

    private void startLoadingData() {
        File directoryPath = new File("aws_json");
        for (File jsonFile : directoryPath.listFiles()) {
            //System.out.println("Submitting file: " + jsonFile.getName());
            LoadBillingDataTask loadBillingDataTask = new LoadBillingDataTask(jsonFile);
            pool.submit(loadBillingDataTask);
            //System.out.println("Submitted file: " + jsonFile.getName());
        }
    }

    PoolingHttpClientConnectionManager connectionManager;

    private void initHttpConnectionConfig() throws KeyManagementException, NoSuchAlgorithmException {
        // Create a trust manager that does not validate certificate chains
        TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return null;
            }

            public void checkClientTrusted(X509Certificate[] certs, String authType) {
            }

            public void checkServerTrusted(X509Certificate[] certs, String authType) {
            }
        }
        };

        // Install the all-trusting trust manager
        SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustAllCerts, new java.security.SecureRandom());

        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

        // Create all-trusting host name verifier
        HostnameVerifier allHostsValid = new HostnameVerifier() {
            public boolean verify(String hostname, SSLSession session) {
                return true;
            }
        };

        // Install the all-trusting host verifier
        HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);

        final SSLConnectionSocketFactory sslSocketFactory = SSLConnectionSocketFactoryBuilder.create()
                .setSslContext(sc)
                .setTlsVersions(TLS.V_1_2)
                .build();

        connectionManager = PoolingHttpClientConnectionManagerBuilder.create()
                .setMaxConnTotal(configuration.getConcurrencyCount())
                .setMaxConnPerRoute(configuration.getConcurrencyCount())
                .setSSLSocketFactory(sslSocketFactory)
                .build();
    }

    private synchronized String getXAuthToken() {

        if (token != null && !token.isTokenExpired() && token.secondsRemainingForTokeExpiry() > 30) {
            return token.getToken().getId();
        }

        try {
            String POST_URL = configuration.getOdlIdmEndpoint() + "/v3.0/tokens";
            URL obj = new URL(POST_URL);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();

            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json");
            con.setRequestProperty("Accept", "application/json");
            con.setDoOutput(true);

            String jsonInputString = "{\n" +
                    "  \"passwordCredentials\" : {\n" +
                    "    \"username\" : \"" + configuration.getOdlDiUsername() + "\",\n" +
                    "    \"password\" : \"" + configuration.getOdlDiPassword() + "\"\n" +
                    "  },\n" +
                    "  \"tenantName\" : \"Provider\"\n" +
                    "}";

            try (OutputStream os = con.getOutputStream()) {
                byte[] input = jsonInputString.getBytes("utf-8");
                os.write(input, 0, input.length);
                os.flush();
            }

            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(con.getInputStream(), "utf-8"))) {
                StringBuilder response = new StringBuilder();
                String responseLine = null;
                while ((responseLine = br.readLine()) != null) {
                    response.append(responseLine.trim());
                }
                String responseString = response.toString();

                ObjectMapper mapper = new ObjectMapper();
                token = mapper.readValue(responseString, XAuthToken.class);
            }
        } catch (Exception e) {
            logger.error("Error", e);
        }

        //System.out.println("x-auth-token : " + token.getToken().getId());
        logger.info("X-Auth-Token : " + token.getToken().getId());
        return token.getToken().getId();
    }

    public int loadJsonDataToODLNew(CloseableHttpClient httpClient, String jsonFile) throws IOException {
        int responseCode = -1;
        String xAuthToken = getXAuthToken();
        Path fileName = Paths.get(jsonFile);
        String jsonBody = new String(Files.readAllBytes(fileName));

        HttpPost httpPost = new HttpPost(configuration.getOdlReceiverEndpoint());
        httpPost.setHeader("X-Auth-Token", xAuthToken);
        httpPost.setHeader("messagetopic", configuration.getOdlDatasetName());
        httpPost.setEntity(new StringEntity(jsonBody, ContentType.APPLICATION_JSON));

        try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
            responseCode = response.getCode();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Error", e);
        }

        return responseCode;
    }

    public int loadJsonDataToODL(String jsonFile) throws IOException, NoSuchAlgorithmException, KeyManagementException {

        HttpURLConnection con = null;
        try {
            String xAuthToken = getXAuthToken();

            String POST_URL = configuration.getOdlReceiverEndpoint(); // "https://odl-master.swinfra.net:30001/itomdi/receiver";
            URL obj = new URL(POST_URL);
            con = (HttpURLConnection) obj.openConnection();

            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json");
            con.setRequestProperty("Accept", "application/json");
            con.setRequestProperty("X-Auth-Token", xAuthToken);
            con.setRequestProperty("messagetopic", configuration.getOdlDatasetName());
//            con.setConnectTimeout( (int) TimeUnit.SECONDS.toMillis(30));
//            con.setReadTimeout( (int) TimeUnit.SECONDS.toMillis(30));

            Path fileName = Paths.get(jsonFile);
            String postJsonData = new String(Files.readAllBytes(fileName));

            // Send post request
            con.setDoOutput(true);
            DataOutputStream wr = new DataOutputStream(con.getOutputStream());
            wr.writeBytes(postJsonData);
            wr.flush();
            wr.close();

            try {
                int responseCode = con.getResponseCode();
                //System.out.println("nSending 'POST' request to URL : " + POST_URL);
                //System.out.println("Post Data : " + postJsonData);
                if (responseCode != 200) {
                    logger.info("Response Code : " + responseCode);
                }
                return responseCode;
                //System.out.println("numberOfRows : "+numberOfRows);
            } catch (IOException e) {
                logger.error("Error", e);
            }
        } catch (Exception e) {
            logger.error("Error", e);
        } finally {
            if (con != null) {
                try {
                    con.disconnect();
                } catch (Exception e) {
                    logger.error("Error", e);
                }
            }
        }
        return -1;
    }


    public int getIngestedRowCount() {
        return rowsIngested.get();
    }

    private void incrementRecordsPushCount(int value) {
        try {
            Instant instant = Instant.now();
            int newValue = rowsIngested.addAndGet(value);
            //System.out.println(instant.toString() + ": Adding new rows" + value + ". Totals rows ingested: " + newValue);
            //pr.println(instant.toString() + ": Rows ingested: " + newValue);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    AtomicInteger failureCounts = new AtomicInteger(0);

    private void incrementFailureCount() {
        int value = failureCounts.incrementAndGet();
        Instant instant = Instant.now();
        logger.info(instant.toString() + ": Total failure count = " + value);
    }


    class LoadBillingDataTask implements Runnable {
        File jsonFile;

        LoadBillingDataTask(File jsonFile) {
            this.jsonFile = jsonFile;
        }

        @Override
        public void run() {
            try {
                CloseableHttpClient httpClient = HttpClients.custom()
                        .setConnectionManager(connectionManager)
                        .build();

//                int responseCode = loadJsonDataToODL(jsonFile.getAbsolutePath());
                int responseCode = loadJsonDataToODLNew(httpClient, jsonFile.getAbsolutePath());
                if (responseCode == 429) {
                    while (responseCode == 429) {
                        incrementFailureCount();
                        logger.info("Received response 429. Waiting to retry");
                        Thread.sleep(5000); // Retry after 5 sec

//                        responseCode = loadJsonDataToODL(jsonFile.getAbsolutePath());
                        responseCode = loadJsonDataToODLNew(httpClient, jsonFile.getAbsolutePath());

                        if (responseCode == 200) {
                            logger.info("Retry succeeded");
                            incrementRecordsPushCount(Util.jsonRecordCount(jsonFile));
                        }
                    }
                } else if (responseCode == 200) {
                    incrementRecordsPushCount(Util.jsonRecordCount(jsonFile));
                } else {
                    logger.info("Failed to send message with error code : " + responseCode);
                }
            } catch (Exception e) {
                logger.error("Error", e);
            }
        }
    }
}
