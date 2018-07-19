package com.metinform.common.process;

import com.simple.datax.core.model.connpara.CommonConnPara;

public abstract interface IDataProcess {
    public abstract void process(String paramString, CommonConnPara paramCommonConnPara)
            throws Exception;
}



