package com.metinform.common.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.metinform.common.BaseConnector;
import com.metinform.common.ConnectorPool;
import com.metinform.common.monitor.ConnectorPoolManager;
import com.metinform.common.param.ConnectParam;
import com.metinform.common.process.IDataProcess;
import com.simple.datax.api.Connector;
import com.simple.datax.api.Receiver;
import com.simple.datax.api.Sender;
import com.simple.datax.core.model.connpara.CommonConnPara;

@Component
public class ScheduledReceiverImpl
        implements Receiver {
    private static final Logger logger = LoggerFactory.getLogger(ScheduledReceiverImpl.class);
    private static boolean threadFlag = true;
    private ConnectParam connectParam = null;
    private Sender sender = null;
    private ConnectorPool connectorPool = ConnectorPoolManager.getInstance().getConnectorPool("RECEIVE", null, null);
    private IDataProcess dataProcess;

    @Override
    public Runnable process(Connector connector, CommonConnPara para, int threshold) {
        return new ProcessRunnable(connector, para, threshold);
    }

    private class ProcessRunnable
            implements Runnable {
        private Connector connector;
        private CommonConnPara para;
        private int threshold;

        public ProcessRunnable(Connector connector, CommonConnPara para, int threshold) {
            this.connector = connector;
            this.para = para;
            if (para.getThreshold() < 1) {
                this.threshold = threshold;
            } else {
                this.threshold = para.getThreshold();
            }
        }

        @Override
        public void run() {
            ScheduledReceiverImpl.logger.debug("Thread: " + Thread.currentThread().getId() + " - threshold:" + threshold);
            try {
                if ((connector != null) && (ScheduledReceiverImpl.isThreadFlag())) {
                    while (threshold > 0) {
                        if (!((BaseConnector) connector).recvMessageWithThreads(dataProcess, para)) {
                            break;
                        }
                        threshold -= 1;
                    }
                }
            } catch (Exception e) {
                ScheduledReceiverImpl.logger.error(
                        "Reciver get message error. Para:" + para.getTransportType() + ":" + para.getTransportPara(), e);
                if (connector != null) {
                    try {
                        connectorPool.returnObject((BaseConnector) connector);
                    } catch (Exception ex) {
                        ScheduledReceiverImpl.logger.error("Connector release error.", ex);
                    }
                }
            } finally {
                if (connector != null) {
                    try {
                        connectorPool.returnObject((BaseConnector) connector);
                    } catch (Exception e) {
                        ScheduledReceiverImpl.logger.error("Connector release error.", e);
                    }
                }
            }
        }
    }

    public ConnectParam getConnectParam() {
        return connectParam;
    }

    public void setConnectParam(ConnectParam connectParam) {
        this.connectParam = connectParam;
    }

    public Sender getSender() {
        return sender;
    }

    public void setSender(Sender sender) {
        this.sender = sender;
    }

    public IDataProcess getDataProcess() {
        return dataProcess;
    }

    public void setDataProcess(IDataProcess dataProcess) {
        this.dataProcess = dataProcess;
    }

    public static boolean isThreadFlag() {
        return threadFlag;
    }

    public static synchronized void setThreadFlag(boolean threadFlag) {
        threadFlag = threadFlag;
    }
}



