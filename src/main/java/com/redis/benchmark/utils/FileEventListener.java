package com.redis.benchmark.utils;

import java.io.File;
import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;

import static com.redis.benchmark.utils.JedisConnectionManagement.firstActiveIndex;

public final class FileEventListener {

    public static final FileEventListener FILE_EVENT_LISTENER = new FileEventListener();

    public FileEventListener() {
    }

    /*
    Apache commons monitoring uses a polling mechanism with a configurable polling interval.
    In every poll, it calls listFiles() method of File class and compares with the listFiles()
    output of the previous iteration to identify file creation, modification and deletion.
    The algorithm is robust enough. Works even on network drives.
     */
    public void start(String dir, int pollInterval) throws Exception {
        FileAlterationObserver observer = new FileAlterationObserver(dir);
        FileAlterationMonitor monitor = new FileAlterationMonitor(pollInterval);
        FileAlterationListener listener = new FileAlterationListenerAdaptor() {
            @Override
            public void onFileCreate(File file) {
                System.out.println("\nDetected file create event. File: " + file);
                int fallBackIndex = Integer.parseInt(file.getName().substring(0, 1));
                firstActiveIndex = fallBackIndex;
                System.out.println("User have requested to fallback to MultiClusterIndex " + fallBackIndex);
                JedisConnectionManagement.provider.setActiveMultiClusterIndex(firstActiveIndex);
            }

            @Override
            public void onFileDelete(File file) {
                System.out.println("\nDetected file delete event. File: " + file);
            }

            @Override
            public void onFileChange(File file) {
                System.out.println("\nDetected file change event. File: " + file);
            }
        };
        observer.addListener(listener);
        monitor.addObserver(observer);
        monitor.start();
        System.out.println("Starting File Listener Service on " + observer.getDirectory());
    }

}