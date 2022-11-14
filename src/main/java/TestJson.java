import com.microfocus.finops.poc.OdlConfiguration;
import com.microfocus.finops.poc.Util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class TestJson {
    static Logger logger = LoggerFactory.getLogger(TestJson.class); // LoggerFactory.getLogger(TestJson.class);

    ExecutorService pool = Executors.newFixedThreadPool(10);
    public static void main(String[] args) throws Exception {

        System.out.println("sdfhsdljfhlsdfdsf");
        logger.info("sdfhsdljfhlsdfdsf");

        TestJson test = new TestJson();
        test.testColumnSplit();
        if (true)
            return;

        test.intiConfigProperties();
//        test.startTestTasks();

        File awsJsonDir = new File("aws_json");
        int totalRecordCount = 0;
        for (File jFile : awsJsonDir.listFiles()) {
            totalRecordCount += test.getJsonRecordCount(jFile);
        }
        System.out.println("Total Records :" + totalRecordCount);
//        File jsonFile = new File("aws_json/rndcostreport-00002-0000.json");
//        File jsonFile2 = new File("aws_json/rndcostreport-00002-7849.json");
//        test.getJsonRecordCount(jsonFile);
//        test.getJsonRecordCount(jsonFile2);
    }

    private int getJsonRecordCount(File file) {
        return Util.jsonRecordCount(file);
    }

    private void intiConfigProperties() throws IOException {
        OdlConfiguration configuration = new OdlConfiguration();

    }

    private  void testColumnSplit() throws FileNotFoundException {
        FileInputStream fstream = new FileInputStream("conf\\allColumns.txt");
        BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
        String line;
        File newFile = new File("dataset/datasetBody.json");
        if (newFile.length() == 0) {
            System.out.println("File is empty ...");
            return;
        }

        PrintWriter pr = null;
        try {
            pr = new PrintWriter(new OutputStreamWriter(new FileOutputStream(newFile, true)));
            while ((line = br.readLine()) != null) {
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
                    System.out.println(fieldName + "--- " + fieldType + "-----" + maxLength);
                }}} catch (Exception e) {

        }
    }

    private void startTestTasks() {
        try {
            int taskCount = 100;
            for (int count=0; count < taskCount; count++) {
                MyTask t1 = new MyTask();
                pool.submit(t1);
                Thread.sleep(2);
            }

//            Thread.sleep(10);
//            MyTask t2 = new MyTask();
//            pool.submit(t2);
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    AtomicInteger counter = new AtomicInteger(0);

    private void incrementCounter(int value) {
        int newValue = counter.addAndGet(value);
        Instant instant = Instant.now();
        System.out.println(instant.toString() + ": incremented value " + newValue);
    }

    class MyTask implements Runnable {
        @Override
        public void run() {
            incrementCounter(10);
        }
    }
}