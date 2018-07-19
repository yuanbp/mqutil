package com.metinform.common;

import com.simple.datax.core.model.connpara.CommonConnPara;

public abstract class ConnectorPool {
    public static final String EXTEND_CHAR = ".";

    public static String getConnKey(String connType, String connPara) {
        if (connType == null) {
            return null;
        }
        int pos = -1;
        if ((pos = connType.indexOf(".")) >= 0) {
            connType = connType.substring(0, pos);
        }
        String connKey = connType.toUpperCase() + ":" + connPara;
        return connKey;
    }

    public abstract BaseConnector borrowObject(CommonConnPara paramCommonConnPara);

    public abstract void returnObject(BaseConnector paramBaseConnector);
}



