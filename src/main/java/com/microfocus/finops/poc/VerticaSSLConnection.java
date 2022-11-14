package com.microfocus.finops.poc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Pattern;

class VerticaSSLConnection {

    private static OdlConfiguration configuration;

    static Logger logger  = LoggerFactory.getLogger(VerticaSSLConnection.class);

    public static void main(String[] args) throws Exception {
        Instant startTime = Instant.now();

        readRowsCount(10000, startTime);

    }

    public static int readRowsCount(int totalRowsCountOfCSV, Instant start) throws Exception {
        configuration = new OdlConfiguration();
        RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();

        // Get name representing the running Java virtual machine.
        // It returns something like 6460@AURORA. Where the value
        // before the @ symbol is the PID.
        String jvmName = bean.getName();
        System.out.println("Name = " + jvmName);

        // Extract the PID by splitting the string returned by the
        // bean.getName() method.
        long pid = Long.valueOf(jvmName.split("@")[0]);
        System.out.println("PID  = " + pid);

        int exitCode = 9;
        try {
            System.out.println("starting program in new jvm process...");

            String hostname = configuration.getVerticaDBHost();
            String port = configuration.getVerticaDBPort();
            String db_instance = configuration.getVerticaDBName();
            String username = configuration.getVerticaDBUsername();
            String password = configuration.getVerticaDBPassword();
            String ssl = String.valueOf(configuration.isVerticaDbSslEnabled());
            String truststore_path = configuration.getVerticaTrustStorePath();
            String truststore_pass = configuration.getVerticaTrustStorePassword();
            String java_trustStorePath = configuration.getJavaTrustStorePath();

            logger.info("args : hostname:" + hostname + " port:" + port + " db_instance:" + db_instance + " username:" + username + " ssl:" + ssl + " truststore_path:" + truststore_path);
            logger.info("trying to load driver");

            Class.forName("com.vertica.jdbc.Driver");
            DriverManager.setLoginTimeout(30);
            String uri = "jdbc:vertica://" + hostname + ":" + port + "/" + db_instance;
            Properties dbProperties = new Properties();
            dbProperties.put("user", username);
            dbProperties.put("password", password);


            if (ssl.equalsIgnoreCase("true")) {
                logger.info("calling loadCertInTruststore function...");
                //loadCertInTruststore(truststore_path, truststore_pass, java_trustStorePath);

                logger.info("connecting to vertica via SSL...");
                dbProperties.put("ssl", true);
                dbProperties.put("TrustStorePath", truststore_path);
                dbProperties.put("TrustStorePassword", truststore_pass);
            } else {
                logger.info("connecting to vertica via non-SSL...");
            }

            try (Connection connection = DriverManager.getConnection(uri, dbProperties)) {
                if (null != connection) {
                    logger.info("connection succeeded");
                    logger.info("Waiting for " + totalRowsCountOfCSV + " messages to be stored in Vertica");
                    Statement stmt = connection.createStatement();

                    ResultSet rs1 = stmt.executeQuery("SELECT count(\"identity/LineItemId\") FROM mf_shared_provider_default."
                            + configuration.getOdlDatasetName());
                    int preExistingRecordCount = 0;
                    if (rs1.next()) {
                        preExistingRecordCount = Integer.parseInt(rs1.getString(1).trim().toString());
                    }

                    int totalRowsOfDB = preExistingRecordCount;
                    while ((totalRowsOfDB - preExistingRecordCount) < totalRowsCountOfCSV) {
                        Instant end = Instant.now();
                        Duration timeElapsed = Duration.between(start, end);
                        long timeDiff = timeElapsed.toMillis();

                        ResultSet rs = stmt.executeQuery("SELECT count(\"identity/LineItemId\") FROM mf_shared_provider_default."
                                + configuration.getOdlDatasetName());

                        while (rs.next()) {
                            totalRowsOfDB = Integer.parseInt(rs.getString(1).trim().toString());
                            int numOfRowsSent = StreamCsvToOdl.rowsIngested.get();
                            double msgPerMin = 0.0;
                            if (timeDiff > 1000) {
                                msgPerMin = (((totalRowsOfDB - preExistingRecordCount) * 60) / (timeDiff / 1000));
                                //msgPerMin = msgPerMin * 60000;
                            }
                            String message = "After " + timeDiff + " ms"
                                    + ", Messages Throughput (msg/min) = " + (int) msgPerMin
                                    + ", Sent to Receiver = " + numOfRowsSent
                                    + ", In Vertica = " + (totalRowsOfDB - preExistingRecordCount)
                                    + ", Remaining to read = " + (numOfRowsSent - (totalRowsOfDB - preExistingRecordCount));

                            logger.info(message);
                        }

                        Thread.sleep(5000);
                    }

                    exitCode = 0;
                } else {
                    logger.info("connection failed");
                }

            } catch (SQLException e) {
//                e.printStackTrace();
                logger.error("Error",e);

                String cause;
                try {
                    cause = e.getCause().toString();
                } catch (Exception e1) {
                    cause = "";
                }
                String pattern = "^.*Database .* does not exist";
                if (e.toString().contains("NonTransientConnectionException")
                        || e.toString().contains("Check that the hostname and port are correct")) {
                    exitCode = 1;
                } else if (e.toString().contains("Unknown host specified")
                        || cause.contains("java.net.UnknownHostException")) {
                    exitCode = 2;
                } else if (e.toString().contains("The Network Adapter could not establish the connection")
                        || e.toString().contains("Check that the hostname and port are correct")
                        || e.toString().contains("java.net.SocketException")) {
                    exitCode = 3;
                } else if (Pattern.matches(pattern, e.toString())) {
                    exitCode = 4;
                } else if (e.toString().contains("The connection attempt failed")) {
                    exitCode = 5;
                } else if (e.toString().contains("SQLInvalidAuthorizationSpecException")) {
                    exitCode = 6;
                } else if (e.toString().contains("is not permitted to log in")) {
                    exitCode = 7;
                } else if (e.toString().contains("javax.net.ssl.SSLHandshakeException")) {
                    exitCode = 8;
                } else {
                    exitCode = 9;
                }
            }
        } catch (Exception e) {
            logger.error("Error",e);
            return (exitCode);
        }
        return (exitCode);
    }

    private static void logmess(String logmes) {
        Date date = new Date();
        try (PrintWriter pr =
                     new PrintWriter(new OutputStreamWriter(new FileOutputStream("VerticaSSLConnection.log", true)))) {
            pr.println(date.toString() + ":" + logmes);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void loadCertInTruststore(String certFilePath, String truststore_path, String truststore_pass) {
        // TODO Auto-generated method stub
        {
            System.out.println("certFilePath :" + certFilePath);
            System.out.println("truststore_path :" + truststore_path);
            System.out.println("truststore_pass :" + truststore_pass);

            try (FileInputStream inStream = new FileInputStream(certFilePath);
                 BufferedInputStream bis1 = new BufferedInputStream(inStream)) {

                int byteNum = bis1.available();
                System.out.println("byteNum" + byteNum);

                String alias_name = "vertica_cert_alias";

                File file = new File(truststore_path);
                try (InputStream localCertIn = new FileInputStream(file)) {
                    KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
                    char[] passphrase = truststore_pass.toCharArray();

                    System.out.println("loading keystore");
                    keystore.load(localCertIn, passphrase);
                    if (keystore.containsAlias(alias_name)) {
                        System.out.println("deleting existing " + alias_name);
                        keystore.deleteEntry(alias_name);

                    }
                    localCertIn.close();

                    CertificateFactory cf = CertificateFactory.getInstance("X.509");
                    while (bis1.available() > 0) {
                        Certificate cert = cf.generateCertificate(bis1);
                        keystore.setCertificateEntry(alias_name, cert);
                    }

                    System.out.println("loading new cert with alias set to :" + alias_name);
                    inStream.close();
                    try (OutputStream out = new FileOutputStream(file)) {
                        System.out.println("adding vertica certificate to truststore...");
                        keystore.store(out, passphrase);
                    }
                }
            } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException e) {
                e.printStackTrace();
            }
        }
    }


}
