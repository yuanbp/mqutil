package com.metinform.common.monitor;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

public class SendThreadPoolExecutor
        extends ThreadPoolTaskExecutor {
    private static SendThreadPoolExecutor instance;
    private Set<String> connectorSet = new HashSet();
    private static final long serialVersionUID = -6635958602858575004L;

    public SendThreadPoolExecutor() {
        initializeExecutor(Executors.defaultThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());
        instance = this;
    }

    public static synchronized SendThreadPoolExecutor getInstance() {
        if (instance == null) {
            instance = new SendThreadPoolExecutor();
        }
        return instance;
    }

    public void execute(Runnable task, String key) {
        if (!isExisted(key)) {
            execute(task);
        }
    }

    private boolean isExisted(String connector) {
        synchronized (connectorSet) {
            if (connectorSet.contains(connector)) {
                return true;
            }
            connectorSet.add(connector);
        }
        return false;
    }

    public void workDone(String connector) {
        synchronized (connectorSet) {
            connectorSet.remove(connector);
        }
    }
}



