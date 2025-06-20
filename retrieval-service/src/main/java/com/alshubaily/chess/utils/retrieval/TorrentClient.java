package com.alshubaily.chess.utils.retrieval;

import bt.Bt;
import bt.data.Storage;
import bt.data.file.FileSystemStorage;
import bt.runtime.BtClient;
import bt.runtime.BtRuntime;
import bt.runtime.Config;

import java.io.File;
import java.net.URL;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TorrentClient {

    private final BtRuntime runtime = BtRuntime.builder(new Config()).build();

    public boolean download(URL torrentUrl, File target, int idleTimeoutSeconds) {
        File downloadDir = target.getParentFile();
        Storage storage = new FileSystemStorage(downloadDir.toPath());

        AtomicLong lastProgressTime = new AtomicLong(System.currentTimeMillis());
        AtomicInteger lastCompleted = new AtomicInteger(0);
        CountDownLatch doneSignal = new CountDownLatch(1);

        BtClient client = Bt.client(runtime)
                .storage(storage)
                .torrent(torrentUrl)
                .stopWhenDownloaded()
                .build();

        Runnable monitorTask = () -> {
            long idleTime = System.currentTimeMillis() - lastProgressTime.get();
            if (idleTime > idleTimeoutSeconds * 1000L) {
                System.out.println("⚠️ Torrent stalled. No progress for " + idleTimeoutSeconds + "s.");
                client.stop();
                doneSignal.countDown();
            }
        };

        try (ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor()) {
            scheduler.scheduleAtFixedRate(monitorTask, idleTimeoutSeconds, idleTimeoutSeconds, TimeUnit.SECONDS);

            Thread torrentThread = new Thread(() -> {
                try {
                    client.startAsync(state -> {
                        int current = state.getPiecesComplete();
                        if (current > lastCompleted.get()) {
                            lastProgressTime.set(System.currentTimeMillis());
                            lastCompleted.set(current);
                        }
                        System.out.printf("Progress: %d / %d%n", current, state.getPiecesTotal());
                    }, 5000).join();
                } finally {
                    doneSignal.countDown();
                }
            });

            torrentThread.start();
            doneSignal.await();
        } catch (InterruptedException e) {
            System.out.println("⚠️ Interrupted during download monitoring.");
            Thread.currentThread().interrupt();
        } finally {
            runtime.shutdown();
        }
        return target.exists() && target.length() > 0;
    }
}
