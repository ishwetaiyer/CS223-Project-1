package cs223.project1;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

class Configuration {
    private String createDbFile;
    private String dropDbFile;
    private List<Workload> workloads = new ArrayList<>();
    private List<DbConfiguration> dbConfigurations = new ArrayList<>();

    private DbConfiguration loggingDbConfiguration;

    List<Workload> getWorkloads() {
        return workloads;
    }


    Configuration() {
        Properties properties = new Properties();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            properties.load(is);
        } catch (IOException e) {
            System.out.println("Properties file not found");
            System.exit(1);
        }

        createDbFile = properties.getProperty("createDbFile");
        dropDbFile = properties.getProperty("dropDbFile");

        boolean mysqlEnabled = Boolean.parseBoolean(properties.getProperty("mysql.enabled"));
        if (mysqlEnabled) {
            DbConfiguration mysqlDbConfig = new DbConfiguration("mysql")
                    .setDbUrl(properties.getProperty("mysql.url"))
                    .setPassword(properties.getProperty("mysql.password"))
                    .setUsername(properties.getProperty("mysql.username"))
                    .setMPL(Integer.parseInt(properties.getProperty("mysql.MPL")));
            dbConfigurations.add(mysqlDbConfig);
        }

        boolean postgresEnabled = Boolean.parseBoolean(properties.getProperty("postgres.enabled"));
        if (postgresEnabled) {
            DbConfiguration postgresDbConfig = new DbConfiguration("postgres")
                    .setDbUrl(properties.getProperty("postgres.url"))
                    .setPassword(properties.getProperty("postgres.password"))
                    .setUsername(properties.getProperty("postgres.username"))
                    .setMPL(Integer.parseInt(properties.getProperty("postgres.MPL")));
            dbConfigurations.add(postgresDbConfig);
        }

        boolean lowConcurrencyWorkloadEnabled = Boolean.parseBoolean(properties.getProperty("lowWorkload.enabled"));
        if (lowConcurrencyWorkloadEnabled) {
            Workload lowConcurrencyWorkload = new Workload("lowConcurrency")
                    .setMetadataFile(properties.getProperty("lowWorkload.metadataFile"))
                    .setWorkloadFile(properties.getProperty("lowWorkload.statementFile"));
            workloads.add(lowConcurrencyWorkload);

        }

        boolean highConcurrencyWorkloadEnabled = Boolean.parseBoolean(properties.getProperty("highWorkload.enabled"));
        if (highConcurrencyWorkloadEnabled) {
            Workload highConcurrencyWorkload = new Workload("highConcurrency")
                    .setMetadataFile(properties.getProperty("highWorkload.metadataFile"))
                    .setWorkloadFile(properties.getProperty("highWorkload.statementFile"));
            workloads.add(highConcurrencyWorkload);
        }

        loggingDbConfiguration = new DbConfiguration("logging")
                .setDbUrl(properties.getProperty("logging.url"))
                .setPassword(properties.getProperty("logging.password"))
                .setUsername(properties.getProperty("logging.username"));

    }

    DbConfiguration getLoggingDbConfiguration() {
        return loggingDbConfiguration;
    }

    private int parseIsolationLevel(String property) {
        // TODO: 2/11/2020 add proper parsing
        return Connection.TRANSACTION_SERIALIZABLE;
    }

    String getCreateDbFile() {
        return createDbFile;
    }

    String getDropDbFile() {
        return dropDbFile;
    }

    List<DbConfiguration> getDbConfigurations() {
        return dbConfigurations;
    }

    public static class Workload {
        private String name;
        private String metadataFile;
        private String workloadFile;

        Workload(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        String getMetadataFile() {
            return metadataFile;
        }

        Workload setMetadataFile(String metadataFile) {
            this.metadataFile = metadataFile;
            return this;
        }

        String getWorkloadFile() {
            return workloadFile;
        }

        Workload setWorkloadFile(String workloadFile) {
            this.workloadFile = workloadFile;
            return this;
        }
    }

    public static class DbConfiguration {
        private String name;
        private String dbUrl;
        private String username;
        private String password;
        private int MPL;
        private int isolationLevel = Connection.TRANSACTION_SERIALIZABLE;

        int getIsolationLevel() {
            return isolationLevel;
        }

        DbConfiguration setIsolationLevel(int isolationLevel) {
            this.isolationLevel = isolationLevel;
            return this;
        }

        DbConfiguration(String name) {
            this.name = name;
        }

        String getDbUrl() {
            return dbUrl;
        }

        DbConfiguration setDbUrl(String dbUrl) {
            this.dbUrl = dbUrl;
            return this;

        }

        String getUsername() {
            return username;
        }

        DbConfiguration setUsername(String username) {
            this.username = username;
            return this;

        }

        String getPassword() {
            return password;

        }

        DbConfiguration setPassword(String password) {
            this.password = password;
            return this;

        }

        public int getMPL() {
            return MPL;
        }

        DbConfiguration setMPL(int MPL) {
            this.MPL = MPL;
            return this;
        }

        public String getName() {
            return name;
        }

    }
}
