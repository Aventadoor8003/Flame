package cis5550.tools;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;

public class TableNameGenerator {
    private static final AtomicInteger sequenceNumber = new AtomicInteger(0);

    /**
     * Generates a unique table name
     * @param usage - a string that describes the usage of the table
     * @return a unique table name
     */
    public static String generateUniqueTableName(String usage) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MMdd_HHmmss_SSS");
        String timestamp = LocalDateTime.now().format(formatter);
        int seq = sequenceNumber.incrementAndGet();
        return "Table_" + usage + "_" + timestamp + "_" + seq;
    }

    public static int getSequenceNumber() {
        return sequenceNumber.incrementAndGet();
    }
}
