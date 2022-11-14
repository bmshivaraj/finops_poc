package com.microfocus.finops.poc;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.io.*;

public class VerticaConnect {
    public static void main(String[] args) {
        int num = 784925;
        try {
            Instant start = Instant.now();
            readRowsCount(num, start);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void logmess(String logmes)
    {
        try{
            PrintWriter pr =
                    new PrintWriter(new OutputStreamWriter (new FileOutputStream("aws_csv\\verticaCount.txt",true)));

//            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
//            LocalDateTime now = LocalDateTime.now();
//            //System.out.println(dtf.format(now));
            Instant instant = Instant.now();
            System.out.println(instant.toString() + " : " + logmes);
            pr.println(instant.toString() + " : " + logmes);
            pr.close();
        }catch(Exception e){
        }
    }

    public static void readRowsCount(int totalRowsCountOfCSV, Instant start) throws IOException {
//        if(true)
//            return;

        /*
         * If your client needs to run under a Java 5 JVM, It will use the older
         * JDBC 3.0-compliant driver, which requires you manually load the
         * driver using Class.forname
         */
        /*
         * try { Class.forName("com.vertica.jdbc.Driver"); } catch
         * (ClassNotFoundException e) { // Could not find the driver class.
         * Likely an issue // with finding the .jar file.
         * System.err.println("Could not find the JDBC driver class.");
         * e.printStackTrace(); return; // Bail out. We cannot do anything
         * further. }
         */
        OdlConfiguration configuration = new OdlConfiguration();

        Properties myProp = new Properties();
        myProp.put("user", configuration.getVerticaDBUsername());
        myProp.put("password", configuration.getVerticaDBPassword());
        myProp.put("loginTimeout", "35");
        //myProp.put("KeystorePath", "c:/keystore/keystore.jks");
        //myProp.put("KeystorePassword", "keypwd");
        //myProp.put("TrustStorePath", "c:/truststore/localstore.jks");
        //myProp.put("TrustStorePassword", "trustpwd");
        Connection conn;
        try {
            String jdbcURL = "jdbc:vertica://" +
                    configuration.getVerticaDBHost() + ":" +
                    configuration.getVerticaDBPort() + "/" +
                    configuration.getVerticaDBName();
            //"jdbc:vertica://odl-master.swinfra.net:30012/itomdb"
            conn = DriverManager.getConnection(
                    jdbcURL, myProp);
            System.out.println("Connected!");

            ResultSet rs = null;

            Statement stmt = conn.createStatement();

            int totalRowsOfDB=0;
            while(totalRowsOfDB<totalRowsCountOfCSV) {
                Thread.sleep(5000);
                Instant end = Instant.now();
                Duration timeElapsed = Duration.between(start, end);
                long timeDiff = timeElapsed.toMillis();
//                System.out.println("Time taken: "+timeDiff +" milliseconds");
//                System.out.println("Time taken: "+ timeElapsed.toSeconds() +" seconds");

                rs = stmt.executeQuery("SELECT count(\"identity/LineItemId\") FROM mf_shared_provider_default."
                        + configuration.getOdlDatasetName());

                while (rs.next()) {
                    totalRowsOfDB = Integer.parseInt(rs.getString(1).trim().toString());
                    int numOfRowsIngested = StreamCsvToOdl.rowsIngested.get();
                    logmess( "After '" + timeDiff + "' ms, Num Rows Ingested = " + numOfRowsIngested + ", Num rows in Vertica = " + totalRowsOfDB + ", Messages in Msg Bus = " + (numOfRowsIngested - totalRowsOfDB));
                }
            }

            conn.close();
        } catch (SQLTransientConnectionException connException) {
            // There was a potentially temporary network error
            // Could automatically retry a number of times here, but
            // instead just report error and exit.
            System.out.print("Network connection issue: ");
            System.out.print(connException.getMessage());
            System.out.println(" Try again later!");
            return;
        } catch (SQLInvalidAuthorizationSpecException authException) {
            // Either the username or password was wrong
            System.out.print("Could not log into database: ");
            System.out.print(authException.getMessage());
            System.out.println(" Check the login credentials and try again.");
            return;
        } catch (SQLException e) {
            // Catch-all for other exceptions
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}