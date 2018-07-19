package com.metinform.common.monitor;

import java.io.File;

import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileListener extends FileAlterationListenerAdaptor {
    private static final Logger log = LoggerFactory.getLogger(FileListener.class);

    @Override
    public void onDirectoryChange(File directory) {
    }

    @Override
    public void onDirectoryCreate(File directory) {
    }

    @Override
    public void onDirectoryDelete(File directory) {
    }

    @Override
    public void onFileChange(File file) {
    }

    @Override
    public void onFileCreate(File file) {
    }

    @Override
    public void onFileDelete(File file) {
    }

    @Override
    public void onStart(FileAlterationObserver observer) {
    }

    @Override
    public void onStop(FileAlterationObserver observer) {
    }

    public void init() {
        String classPath = FileListener.class.getClassLoader().getResource("").getPath();
        log.info("========>> Listener Path:" + classPath);
        FileObserver ob = new FileObserver(classPath);
        FileListener listener = new FileListener();
        ob.addListener(listener);
        FileMonitor monitor = new FileMonitor(ob);
        monitor.start();
    }
}



