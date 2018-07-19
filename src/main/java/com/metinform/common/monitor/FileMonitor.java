package com.metinform.common.monitor;

import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;

public class FileMonitor {
    FileAlterationMonitor monitor = null;

    public FileMonitor(FileAlterationObserver ob) {
        this(1000L, ob);
    }

    public FileMonitor(long interval, FileAlterationObserver ob) {
        monitor = new FileAlterationMonitor(interval, new FileAlterationObserver[]{ob});
    }

    public void addObserver(FileAlterationObserver observer) {
        monitor.addObserver(observer);
    }

    public void removeObserver(FileAlterationObserver observer) {
        monitor.removeObserver(observer);
    }

    public Iterable<FileAlterationObserver> getObservers() {
        return monitor.getObservers();
    }

    public void start() {
        try {
            monitor.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        try {
            monitor.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}



