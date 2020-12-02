package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Exchanger;

import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

public class FileWriter
        implements Runnable {
    private static final Logger logger = getLogger(FileWriter.class);
    private String resultFileName;
    private Exchanger<List<Pair<String, Integer>>> exchanger;

    public FileWriter(String resultFileName, Exchanger<List<Pair<String, Integer>>> ex) {
        this.resultFileName = resultFileName;
        this.exchanger = ex;
    }

    @Override
    public void run() {
        logger.info("Started writer thread {}", currentThread().getName());
        try {
            BufferedWriter writer = new BufferedWriter(new java.io.FileWriter(resultFileName));
            while (!currentThread().isInterrupted()) {
                List<Pair<String, Integer>> pairList = exchanger.exchange(null);

                for (Pair<String, Integer> pair : pairList) {
                    String result = pair.getLeft() + " " + pair.getRight();
                    writer.write(result + "\n");
                }
            }
            writer.flush();

            logger.info("Finish writer thread {}", currentThread().getName());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}