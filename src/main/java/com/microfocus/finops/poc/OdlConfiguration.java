package com.microfocus.finops.poc;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class OdlConfiguration {
    private Properties properties = new Properties();

    private String odlIdmEndpoint;
    private String odlDiUsername;
    private String odlDiPassword;
    private String odlDiTenant;
    private String odlReceiverEndpoint;
    private String odlDatasetName;
    private String verticaDBHost;
    private String verticaDBPort;
    private String verticaDBName;
    private String verticaDBUsername;
    private String verticaDBPassword;
    private int concurrencyCount;
    private  boolean verticaDbSslEnabled;
    private String verticaTrustStorePath;
    private String verticaTrustStorePassword;

    public String getJavaTrustStorePassword() {
        return javaTrustStorePassword;
    }

    private String javaTrustStorePassword;

    public String getJavaTrustStorePath() {
        return javaTrustStorePath;
    }

    private String javaTrustStorePath;

    private int messagesPerBatch = 500;


    public OdlConfiguration() throws IOException {
        FileInputStream fis = new FileInputStream("conf/config.properties");
        properties.load(fis);
//        properties.load(OdlConfiguration.class.getClassLoader().getResourceAsStream("config.properties"));

        odlIdmEndpoint = properties.getProperty("odl.idm.endpoint");
        odlDiUsername = properties.getProperty("odl.di.username");
        odlDiPassword = properties.getProperty("odl.di.password");
        odlDiTenant = properties.getProperty("old.di.tenant");
        odlReceiverEndpoint = properties.getProperty("odl.receiver.endpoint");
        odlDatasetName = properties.getProperty("odl.dataset.name");

        verticaDBHost = properties.getProperty("vertica.db.host");
        verticaDBPort = properties.getProperty("vertica.db.port");
        verticaDBName = properties.getProperty("vertica.db.name");
        verticaDBUsername = properties.getProperty("vertica.db.username");
        verticaDBPassword = properties.getProperty("vertica.db.password");
        verticaDbSslEnabled = Boolean.parseBoolean(properties.getProperty("vertica.db.ssl"));
        verticaTrustStorePath = properties.getProperty("truststore.path");
        verticaTrustStorePassword = properties.getProperty("truststore.password");
        javaTrustStorePath = properties.getProperty("java.truststore.path");
        javaTrustStorePassword = properties.getProperty("java.truststore.password");

        messagesPerBatch = Integer.parseInt(properties.getProperty("messages.per.batch"));
        concurrencyCount = Integer.parseInt(properties.getProperty("tool.concurrency"));

    }

    public String getOdlIdmEndpoint() {
        return odlIdmEndpoint;
    }

    public String getOdlDiUsername() {
        return odlDiUsername;
    }

    public String getOdlDiPassword() {
        return odlDiPassword;
    }

    public String getOdlDiTenant() {
        return odlDiTenant;
    }

    public String getOdlReceiverEndpoint() {
        return odlReceiverEndpoint;
    }

    public String getOdlDatasetName() {
        return odlDatasetName;
    }

    public String getVerticaDBHost() {
        return verticaDBHost;
    }

    public String getVerticaDBPort() {
        return verticaDBPort;
    }

    public String getVerticaDBName() {
        return verticaDBName;
    }

    public String getVerticaDBUsername() {
        return verticaDBUsername;
    }

    public String getVerticaDBPassword() {
        return verticaDBPassword;
    }

    public int getMessagesPerBatch() {
        return messagesPerBatch;
    }

    public int getConcurrencyCount() {
        return concurrencyCount;
    }

    public boolean isVerticaDbSslEnabled() {
        return verticaDbSslEnabled;
    }

    public String getVerticaTrustStorePath() {
        return verticaTrustStorePath;
    }

    public String getVerticaTrustStorePassword() {
        return verticaTrustStorePassword;
    }
}
