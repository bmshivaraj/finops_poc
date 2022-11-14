package com.microfocus.finops.poc;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.io.*;

public class JsonToDataset {

    static Logger logger = LoggerFactory.getLogger(JsonToDataset.class);

    OdlConfiguration configuration;
    public JsonToDataset() throws IOException {
        configuration = new OdlConfiguration();
    }

    public static void main(String args[]) throws IOException, NoSuchAlgorithmException, KeyManagementException {
        JsonToDataset jsonToDataset = new JsonToDataset();

        System.out.println("Enter dataset name:");
        Scanner sc=new Scanner(System.in);
        String datasetName = sc.nextLine();

        jsonToDataset.jsonToDatasetConversionAndIssieODLDataSetConfigurationCmd(datasetName);
    }

    public void jsonToDatasetConversionAndIssieODLDataSetConfigurationCmd(String datasetName) throws IOException, KeyManagementException, NoSuchAlgorithmException {
        logger.info("Creating dataset with "+datasetName +" in " + configuration.getOdlReceiverEndpoint());
        File datasetDir = new File("dataset");
        if (!datasetDir.exists()  || !datasetDir.isDirectory()) {
            logger.info("The target directory of dataset is not present. Creating dataset directory" );
            datasetDir.mkdirs();
        }

        if (datasetDir.exists()  && datasetDir.isDirectory() && datasetDir.list() != null) {
            logger.info("Cleaning old data set files.");
            FileUtils.cleanDirectory(datasetDir);
        }

//        FileInputStream fstream = new FileInputStream("conf\\allColumns.txt");
//        BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
//
//        String strLine;
//
////        //Read File Line By Line
//        while ((strLine = br.readLine()) != null)   {
//            generateFiledBody();
//        }
        generateFiledBody();

        Path fileName = Paths.get("dataset\\datasetBody.json");

        String datasetFields = new String(Files.readAllBytes(fileName)); // .readString(fileName);

        String payload = "{\n" +
                "  \"id\": \""+datasetName+"\",\n" +
                "  \"name\": \""+datasetName+"\",\n" +
                "  \"displayName\": \""+datasetName+"\",\n" +
                "  \"description\": \"Sample dataset for demo\",\n" +
                "  \"category\": \"Org1_extendedNodeData\",\n" +
                "  \"entityTypeCanonicalName\": \"node\",\n" +
                "  \"fields\": [" + datasetFields +"],\n" +
                "  \"sortOrder\": [\n" +
                "    \"lineItem/ProductCode\"\n" +
                "  ],\n" +
                "  \"shardDefinition\": {\n" +
                "    \"shardType\": \"distribute\",\n" +
                "    \"shardKeys\": [\n" +
                "      \"lineItem/ProductCode\"\n" +
                "    ]\n" +
                "  }\n" +
                "}";

        //System.out.println("payload:" + payload);
        logmess1(payload);
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

        String POST_URL = "https://odl-master.swinfra.net:19443/itom-data-ingestion-administration/urest/v3/dataSetConfiguration";
        URL obj = new URL(POST_URL);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();

        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type", "application/json");
        con.setRequestProperty("Accept", "application/json");
        con.setRequestProperty("X-Auth-Token", fetchXAuthToken()); // "eyJ0eXAiOiJKV1MiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIyYzkwODM4YjgzY2M2YjFlMDE4M2NjNmZjNWMwMDAwZSIsImlzcyI6IklkTSAxLjM2LjAtYnVpbGQuMjYzIiwiY29tLmhwZS5pZG06dHJ1c3RvciI6bnVsbCwiZXhwIjoxNjY2MjczNTMwLCJjb20uaHAuY2xvdWQ6dGVuYW50Ijp7ImlkIjoiMmM5MDgyOGI4M2NjNmIyMDAxODNjYzZiNjBlNzAwY2EiLCJuYW1lIjoiUHJvdmlkZXIiLCJlbmFibGVkIjp0cnVlfSwicHJuIjoiZGlhZG1pbiIsImlhdCI6MTY2NjI3MTczMCwianRpIjoiYTljMmFjNGMtODEyZS00Y2Y3LWJmZDMtYzNmMDc2NzY5N2NmIn0.nC7C1JJ9jTE4aDXUpM3JhNRcr9capcLKTTRe2TXcrcg");
        con.setDoOutput(true);

        //if(true) return;
        try(OutputStream os = con.getOutputStream()) {
            byte[] input = payload.getBytes("utf-8");
            os.write(input, 0, input.length);
        }

        try {
            int responseCode = con.getResponseCode();
            System.out.println("nSending 'POST' request to URL : " + POST_URL);
            //System.out.println("Post Data : " + postJsonData);
            System.out.println("Response Code : " + responseCode);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //Close the input stream
    }

    private static void logmess1(String payload) {

        try {
            PrintWriter pr =
                    new PrintWriter(new OutputStreamWriter(new FileOutputStream("dataset\\finalDatasetBody.json", true)));
            pr.println(payload);
            pr.close();
        } catch (Exception e) {
        }
    }

    private static void generateFiledBody() throws IOException {

        FileInputStream fstream = new FileInputStream("conf\\allColumns.txt");
        BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
        String line;
        File newFile = new File("dataset/datasetBody.json");
        if (newFile.isFile() && newFile.exists() ) {
            FileUtils.delete(newFile);
        }

        PrintWriter pr = null;
        try {
            pr = new PrintWriter(new OutputStreamWriter(new FileOutputStream(newFile, true)));
            line = br.readLine();
            while (line != null) {
                List<String> list = new ArrayList<>();
                if (!list.contains(line)) {
                    String[] lineDetails = line.split(":");

                    String fieldName = lineDetails[0];
                    String fieldType = "string";
                    String maxLength = "255";
                    if (lineDetails.length > 1) {
                        fieldType = lineDetails[1];
                    }
                    if(lineDetails.length > 2) {
                        maxLength = lineDetails[2];
                    }
                    System.out.println("creating field " + line);
                    String str = "{\n" +
                            "      \"id\": \"" + fieldName + "\",\n" +
                            "      \"name\": \"" + fieldName + "\",\n" +
                            "      \"type\": \"" + fieldType + "\",\n" +
                            "      \"displayName\": \"" + fieldName + "\",\n" +
                            "      \"description\": \"" + fieldName + "\",\n" +
                            "      \"maxLength\": " + maxLength +",\n" +
                            "      \"isKey\": true,\n" +
                            "      \"isPrimaryKey\": false,\n" +
                            "      \"isSampleTime\": false,\n" +
                            "      \"isNullable\": true,\n" +
                            "      \"entityTypeCanonicalName\": \"node\",\n" +
                            "      \"fieldCanonicalName\": \"hostname\"\n" +
                            "    }";
                    pr.println(str);
                    line = br.readLine();
                    if(line != null) {
                        pr.print(",");
                    }
                } else {
                    System.out.println("already exists :" + line);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (pr != null) {
                pr.close();
            }
        }
    }

//    private static void createJsonContent(String strLine) {
////        //parse line for :
////        String[] str = strLine.split("\":");
////        String firstpart = str[0];
////        String[] str1 = firstpart.split("\"");
////        //System.out.println(str1[1]);
//        logmess(strLine);
//    }


    private static String fetchXAuthToken() {
        {
            String xAuthToken="";
            try {

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

                String POST_URL = "https://odl-master.swinfra.net:19443/idm-service/v3.0/tokens";
                URL obj = new URL(POST_URL);
                HttpURLConnection con = (HttpURLConnection) obj.openConnection();

                con.setRequestMethod("POST");
                con.setRequestProperty("Content-Type", "application/json");
                con.setRequestProperty("Accept", "application/json");
                con.setDoOutput(true);

                String jsonInputString = "{\n" +
                        "  \"passwordCredentials\" : {\n" +
                        "    \"username\" : \"diadmin\",\n" +
                        "    \"password\" : \"1ISO*help\"\n" +
                        "  },\n" +
                        "  \"tenantName\" : \"Provider\"\n" +
                        "}";

                try(OutputStream os = con.getOutputStream()) {
                    byte[] input = jsonInputString.getBytes("utf-8");
                    os.write(input, 0, input.length);
                }

                try(BufferedReader br = new BufferedReader(
                        new InputStreamReader(con.getInputStream(), "utf-8"))) {
                    StringBuilder response = new StringBuilder();
                    String responseLine = null;
                    while ((responseLine = br.readLine()) != null) {
                        response.append(responseLine.trim());
                    }
                    System.out.println(response.toString());
                    String responseString = response.toString();
                    String strTokens[] = responseString.split("\"");
                    xAuthToken = strTokens[5];

                }

            } catch (ProtocolException e) {
                e.printStackTrace();
            } catch (MalformedURLException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (KeyManagementException e) {
                e.printStackTrace();
            }
            System.out.println("x-auth-token : "+xAuthToken);
            return xAuthToken;
        }
    }

}
