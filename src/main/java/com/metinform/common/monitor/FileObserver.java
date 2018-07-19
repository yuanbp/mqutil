package com.metinform.common.monitor;

import java.io.File;
import java.io.FileFilter;

import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationObserver;

public class FileObserver extends FileAlterationObserver {
    private static final long serialVersionUID = 3637219592248717850L;

    public FileObserver(String directory) {
        this(new File(directory), null);
    }

    public FileObserver(File fileName, FileFilter fileFilter) {
        super(fileName, fileFilter, null);
    }

    @Override
    public void initialize()
            throws Exception {
        super.initialize();
    }

    @Override
    public void destroy()
            throws Exception {
        super.destroy();
    }

    @Override
    public void checkAndNotify() {
        super.checkAndNotify();
    }

    @Override
    public void addListener(FileAlterationListener listener) {
        super.addListener(listener);
    }

    @Override
    public void removeListener(FileAlterationListener listener) {
        super.removeListener(listener);
    }

    @Override
    public Iterable<FileAlterationListener> getListeners() {
        return super.getListeners();
    }
}



