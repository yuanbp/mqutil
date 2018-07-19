package com.metinform.common;

import com.metinform.common.process.IDataProcess;
import com.simple.datax.core.model.connpara.CommonConnPara;

public class DataProcThread
        implements Runnable {
    private IDataProcess dataProcess;
    private String xml;
    private CommonConnPara para;

    public DataProcThread(IDataProcess dataProcess, String xml, CommonConnPara para) {
        this.dataProcess = dataProcess;
        this.xml = xml;
        this.para = para;
    }

    @Override
    public void run() {
        try {
            dataProcess.process(xml, para);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public CommonConnPara getPara() {
        return para;
    }

    public void setPara(CommonConnPara para) {
        this.para = para;
    }
}



