package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;

import static java.lang.Runtime.getRuntime;
import static java.nio.charset.Charset.defaultCharset;
import static org.slf4j.LoggerFactory.getLogger;

public class FileProcessor {
    private static final Logger logger = getLogger(FileProcessor.class);
    public static final int CHUNK_SIZE = 2 * getRuntime().availableProcessors();

    public void process(@Nonnull String processingFileName, @Nonnull String resultFileName) {
        checkFileExists(processingFileName);

        final File file = new File(processingFileName);
        ExecutorService fileWriterService = Executors.newFixedThreadPool(CHUNK_SIZE);
        Exchanger<List<Pair<String, Integer>>> ex = new Exchanger<>();
        Thread writeThread = new Thread(new FileWriter(resultFileName, ex));
        writeThread.start();
        LineProcessor lineProcessor = new LineCounterProcessor();
        Phaser phaser = new Phaser(CHUNK_SIZE);

        try (final Scanner scanner = new Scanner(file, defaultCharset())) {
            while (scanner.hasNext()) {
                List<String> list = new ArrayList<>();
                List<Pair<String, Integer>> pairList = new ArrayList<>();
                while (list.size() < CHUNK_SIZE && scanner.hasNext()) {
                    list.add(scanner.nextLine());
                }

                for (int i = 0; i < list.size(); i++) {
                    int temp = i;
                    fileWriterService.submit(() -> {
                        pairList.set(temp, lineProcessor.process(list.get(temp)));
                    });
                    phaser.arrive();
                }

                phaser.arriveAndAwaitAdvance();
                ex.exchange(pairList);
            }
        } catch (IOException | InterruptedException exception) {
            logger.error("", exception);
        }

        writeThread.interrupt();
        fileWriterService.shutdown();

        logger.info("Finish main thread {}", Thread.currentThread().getName());
    }

    private void checkFileExists(@Nonnull String fileName) {
        final File file = new File(fileName);
        if (!file.exists() || file.isDirectory()) {
            throw new IllegalArgumentException("File '" + fileName + "' not exists");
        }
    }
}