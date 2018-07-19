package com.metinform.common.param;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.simple.datax.core.model.connpara.CommonConnPara;

public class ConnectParam {
    private List<Map<String, Object>> recvConnParamlist = null;
    private List<Map<String, Object>> sendConnParamlist = null;
    public List<CommonConnPara> recvConnList = null;
    private Map<Long, CommonConnPara> recvConnMap = null;
    private Map<Long, CommonConnPara> sendConnMap = null;
    private Map<String, String> interfaceClass = null;

    public void init() {
        initRecvConnParam();
        initSendConnParam();
    }

    public void initRecvConnParam() {
        Long channelID = null;
        String connParam = null;
        String communType = null;
        if (recvConnMap == null) {
            recvConnMap = new HashMap();
        }
        if (recvConnList == null) {
            recvConnList = new ArrayList();
        }
        if (recvConnParamlist != null) {
            CommonConnPara cp = null;
            for (Map<String, Object> map : recvConnParamlist) {
                channelID = Long.valueOf(Long.parseLong((String) map.get("channel_id")));
                connParam = (String) map.get("param");
                communType = (String) map.get("commun_type");
                cp = new CommonConnPara();
                cp.setChannelId(channelID.longValue());
                cp.setTransportPara(connParam);
                cp.setTransportType(communType);
                recvConnMap.put(channelID, cp);
                recvConnList.add(cp);
            }
        }
    }

    public void initSendConnParam() {
        Long channelID = null;
        String connParam = null;
        String communType = null;
        if (sendConnMap == null) {
            sendConnMap = new HashMap();
        }
        if (recvConnParamlist != null) {
            CommonConnPara cp = null;
            for (Map<String, Object> map : sendConnParamlist) {
                channelID = Long.valueOf(Long.parseLong((String) map.get("channel_id")));
                connParam = (String) map.get("param");
                communType = (String) map.get("commun_type");
                cp = new CommonConnPara();
                cp.setChannelId(channelID.longValue());
                cp.setTransportPara(connParam);
                cp.setTransportType(communType);
                sendConnMap.put(channelID, cp);
            }
        }
    }

    public List<Map<String, Object>> getRecvConnParamlist() {
        return recvConnParamlist;
    }

    public void setRecvConnParamlist(List<Map<String, Object>> recvConnParamlist) {
        this.recvConnParamlist = recvConnParamlist;
    }

    public List<Map<String, Object>> getSendConnParamlist() {
        return sendConnParamlist;
    }

    public void setSendConnParamlist(List<Map<String, Object>> sendConnParamlist) {
        this.sendConnParamlist = sendConnParamlist;
    }

    public List<CommonConnPara> getRecvConnList() {
        return recvConnList;
    }

    public void setRecvConnList(List<CommonConnPara> recvConnList) {
        this.recvConnList = recvConnList;
    }

    public Map<Long, CommonConnPara> getRecvConnMap() {
        return recvConnMap;
    }

    public void setRecvConnMap(Map<Long, CommonConnPara> recvConnMap) {
        this.recvConnMap = recvConnMap;
    }

    public Map<Long, CommonConnPara> getSendConnMap() {
        return sendConnMap;
    }

    public void setSendConnMap(Map<Long, CommonConnPara> sendConnMap) {
        this.sendConnMap = sendConnMap;
    }

    public Map<String, String> getInterfaceClass() {
        return interfaceClass;
    }

    public void setInterfaceClass(Map<String, String> interfaceClass) {
        this.interfaceClass = interfaceClass;
    }
}



