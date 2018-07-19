package com.metinform.common;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesUtils.class);
    private static PropertyProvider propertyProvider;

    public PropertiesUtils(PropertyProvider aPPropertyProvider) {
        setProp(aPPropertyProvider);
    }

    public static void setProp(PropertyProvider ap) {
        propertyProvider = ap;
    }

    public static String getProperty(String aPKey) {
        if (propertyProvider != null) {
            return propertyProvider.getProperty(aPKey != null ? aPKey.trim() : "");
        }
        return "";
    }

    public void validateWebServiceURL(URL aPURL) {
        try {
            LOGGER.info("URL =&gt; " + aPURL);
            LOGGER.info("URL Path=&gt; " + aPURL.getPath());
            HttpURLConnection httpURLConnection = (HttpURLConnection) aPURL.openConnection();
            httpURLConnection.setConnectTimeout(3000);
            httpURLConnection.connect();
            LOGGER.info("Getting response code");
            int responseCode = httpURLConnection.getResponseCode();
            LOGGER.info("response code = " + responseCode);
            LOGGER.info("disconnecting");
            httpURLConnection.disconnect();
            LOGGER.info("disconnected");
        } catch (MalformedURLException ex) {
            LOGGER.error("MalFormed Exception =&gt; " + ex.getMessage());
        } catch (IOException ex) {
            LOGGER.error("IOException Exception =&gt; " + ex.getMessage());
        }
    }
}



