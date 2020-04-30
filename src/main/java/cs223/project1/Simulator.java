package cs223.project1;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class Simulator {

    private Configuration config;
    private ScheduledExecutorService executor = null;
    private int id = 1;

    private Connection conn;
    private Connection loggingConnection;

    Simulator(Configuration config) {

        this.config = config;
        testDbConnections();

        // Initialize db to a clean state
//        try {
//            dropDb();
//            createDb();
//        } catch (SQLException | IOException e) {
//            e.printStackTrace();
//            System.exit(1);
//        }

        // initialize executor
    }

    private void dropLogging() throws IOException, SQLException {
        runQueries(loggingConnection, "src/resources/dropLogging.sql");
    }

    private void runQueries(Connection conn, String sqlFilePath) throws IOException, SQLException {
        Path path = Paths.get(sqlFilePath);
        String fileContent = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);

        String querySeparator = "([^;]*);";
        Pattern pattern = Pattern.compile(querySeparator);
        Matcher matcher = pattern.matcher(fileContent);
        while (matcher.find()) {
            executeQuery(conn, matcher.group().replace('\n', ' '));
        }
    }

    private void executeQuery(Connection connection, String query) throws SQLException {
        Statement s = connection.createStatement();
        s.execute(query);
    }

    private void insertMetadata(String file) throws IOException, SQLException {
        runQueries(file);
    }

    private void runQueries(String sqlFilePath) throws IOException, SQLException {
        runQueries(conn, sqlFilePath);
    }

    private void dropDb() throws SQLException, IOException {
        runQueries(config.getDropDbFile());
    }

    private void createDb() throws IOException, SQLException {
        String queryFile = config.getCreateDbFile();
        runQueries(queryFile);
    }

    private void testDbConnections() {
        for (Configuration.DbConfiguration dbConfiguration : config.getDbConfigurations()) {
            try {
                testConnection(dbConfiguration);
                System.out.println("Connected to " + dbConfiguration.getName() + " successfully");
            } catch (SQLException e) {
                e.printStackTrace();
                System.out.println("Could not connect to " + dbConfiguration.getName());
                System.exit(1);
            }
        }

        Configuration.DbConfiguration loggingConfig = config.getLoggingDbConfiguration();
        try {
            testConnection(loggingConfig);
            System.out.println("Connected to " + loggingConfig.getName() + " successfully");
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("Could not connect to " + loggingConfig.getName());
            System.exit(1);
        }
    }

    private void testConnection(Configuration.DbConfiguration dbConfig) throws SQLException {
        Connection connection = DriverManager.getConnection(
                dbConfig.getDbUrl(),
                dbConfig.getUsername(),
                dbConfig.getPassword());
        connection.setTransactionIsolation(dbConfig.getIsolationLevel());
        Statement statement = connection.createStatement();
        statement.execute("select 1;");
    }

    void simulate() throws IOException, SQLException {

        // create common logging connection
        Configuration.DbConfiguration loggingConfig = config.getLoggingDbConfiguration();
        loggingConnection = DriverManager.getConnection(
                loggingConfig.getDbUrl(),
                loggingConfig.getUsername(),
                loggingConfig.getPassword()
        );

        dropLogging();

        // simulate for different levels of MPL
        int[] threadCounts = {20};
        int[] transactionSizes = {10000};

        Integer[] consistencyLevels = {Connection.TRANSACTION_READ_COMMITTED};
        int i = 0;
        for (Configuration.DbConfiguration dbConfig : config.getDbConfigurations()) {
            for (int transactionSize : transactionSizes) {
                for (int threadCount : threadCounts) {
                    for (Integer consistency : consistencyLevels) {

                        for (Configuration.Workload workload : config.getWorkloads()) {
                            System.gc();
                            System.out.println("Simulation " + i++ + " starting.");
                            String simulationName = prepareSimulationName(workload.getName(), threadCount, consistency, transactionSize, dbConfig.getName());

                            // create connection.
                            conn = DriverManager.getConnection(
                                    dbConfig.getDbUrl(),
                                    dbConfig.getUsername(),
                                    dbConfig.getPassword());

                            conn.setTransactionIsolation(consistency);


                            // sanitize db before each simulation
                            dropDb();
                            createDb();
                            insertMetadata(workload.getMetadataFile());

                            // parse transactions from file
                            List<Transaction> transactionList = FileParser.createTransactions(conn, workload.getWorkloadFile(), transactionSize);
                            giveTransactionsToThreadPool(threadCount, simulationName, transactionList);
                        }
                    }
                }
            }
        }
    }

    private String prepareSimulationName(String workloadName, int threadCount, int isolationLevel,
                                         int transactionBatchSize, String dbName) {
        id++;
        return String.format("%03d:%s_%d_%d_%d_%s", id, workloadName, threadCount, isolationLevel, transactionBatchSize, dbName);
    }

    private void giveTransactionsToThreadPool(int threadCount, String simulationName, List<Transaction> transactions) {
        // create thread pool
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(simulationName + "%d").build();
        executor = Executors.newScheduledThreadPool(threadCount, threadFactory);

        int minItemsPerThread = transactions.size() / threadCount;
        int maxItemsPerThread = minItemsPerThread + 1;
        int threadsWithMaxItems = transactions.size() - threadCount * minItemsPerThread;
        int currentItem = 0;

        long startTime = System.nanoTime(), endtime = 0;
        CountDownLatch latch = new CountDownLatch(threadCount);

        createSimulationTimeEntry(simulationName, transactions.size());
        for (int i = 0; i < threadCount; i++) {
            int itemCount = (i < threadsWithMaxItems ? maxItemsPerThread : minItemsPerThread);
            int end = currentItem + itemCount;
            final int start = currentItem;
            executor.submit(() -> {
                try {
                    System.out.println("Submitting from " + start + " to " + end);
                    processTransactionsWithinEachThread(latch, transactions.subList(start, end));
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new RuntimeException("Error running transaction");
                }
            });
            currentItem = end;
        }
        System.out.println("done");
        try {
            latch.await();
            System.out.println("waiting done");

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        endtime = System.nanoTime();
        updateSimulationTime(simulationName, endtime - startTime);

    }

    private void processTransactionsWithinEachThread(CountDownLatch latch, List<Transaction> transactions) throws SQLException {
        while (!transactions.isEmpty()) {
            Transaction t = transactions.remove(0);
            executeTransaction(t);
            System.out.println("Thread " + Thread.currentThread().getName() + " done.");
        }
//        for (Transaction t : transactions) {
//            System.out.println("Thread " + Thread.currentThread().getName() + " starting.");
//            executeTransaction(t);
//            System.out.println("Thread " + Thread.currentThread().getName() + " done.");
//        }
        latch.countDown();
    }

    private void updateSimulationTime(String simulationName, long timeDiff) {
        String logStatement = String.format("UPDATE simulation_logging set running_time = %d where id = %d;",
                timeDiff, id);
        try {
            executeQuery(loggingConnection, logStatement);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private void createSimulationTimeEntry(String simulationName, long transaction_size) {
        String logStatement = String.format("INSERT into simulation_logging (ID, name, running_time, transaction_size) values (%d, '%s', %d, %d)",
                id, simulationName, (long) 0, transaction_size);
        try {
            executeQuery(loggingConnection, logStatement);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void executeTransaction(Transaction t) {
        try {
            Statement s = conn.createStatement();
            s.execute("BEGIN");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        long startTime = 0, endTime = 0;
        for (CustomStatement c : t.getStatements()) {
            if (c.isSelectType()) {
                startTime = System.nanoTime();

            }
            try {
                conn.prepareStatement(c.getStatement()).execute();
            } catch (SQLException ex) {
                ex.printStackTrace();
                System.out.println(c.getStatement());
            }
            if (c.isSelectType()) {
                endTime = System.nanoTime();
                logQuery(endTime - startTime);
            }

        }
        try {
            Statement c = conn.createStatement();
            c.execute("COMMIT;");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void logQuery(long timeDifference) {
        String logStatement = String.format("INSERT into query_logging (simulation_id, running_time) values (%d, %d)",
                Integer.parseInt(Thread.currentThread().getName().substring(0, 3)), timeDifference);
        try {
            executeQuery(loggingConnection, logStatement);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
