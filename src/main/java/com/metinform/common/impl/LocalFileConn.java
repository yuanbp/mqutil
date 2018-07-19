package com.metinform.common.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.metinform.common.BaseConnector;
import com.simple.datax.SimpleMessage;
import com.simple.datax.api.ConnectorException;

public class LocalFileConn
        extends BaseConnector {
    private static final Logger logger = LoggerFactory.getLogger(LocalFileConn.class);
    public static final String TYPE = "FILE";
    private static final String PREFIX_DIR = "../activeDB/";
    private String directory;
    private File dir;

    public LocalFileConn(String connPara)
            throws ConnectorException {
        super(connPara);
        this.connPara = connPara;
        parseConnPara();
        init();
    }

    private void init()
            throws ConnectorException {
        try {
            dir = new File(directory);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            if (!dir.isDirectory()) {
                throw new ConnectorException("Specified connPara is not directory failed");
            }
        } catch (Exception ex) {
            throw new ConnectorException("Init local file directory failed", ex);
        }
    }

    @Override
    protected void prepareReceive()
            throws ConnectorException {
        if ((dir == null) || (!dir.isDirectory())) {
            init();
        }
    }

    private void parseConnPara() {
        directory = ("../activeDB/" + connPara);
    }

    @Override
    public void disconnect() {
    }

    @Override
    public String getMessage()
            throws ConnectorException {
        prepareReceive();
        File[] files = dir.listFiles();
        String ret = null;
        if ((files != null) && (files.length > 0)) {
            for (File file : files) {
                if (file.isFile()) {
                    BufferedReader bufReader = null;
                    try {
                        bufReader = new BufferedReader(new InputStreamReader(new FileInputStream(file), getEncoding()));
                        StringBuilder stringBuilder = new StringBuilder();
                        String line = null;
                        String ls = System.getProperty("line.separator");
                        while ((line = bufReader.readLine()) != null) {
                            stringBuilder.append(line);
                            stringBuilder.append(ls);
                        }
                        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
                        ret = stringBuilder.toString();
                    } catch (FileNotFoundException e) {
                        logger.error("File Name - " + file.getName(), e);
                        if (bufReader != null) {
                            try {
                                bufReader.close();
                            } catch (IOException ex) {
                                logger.debug("", ex);
                            }
                        }
                    } catch (UnsupportedEncodingException e) {
                        logger.error("encoding - " + getEncoding(), e);
                        if (bufReader != null) {
                            try {
                                bufReader.close();
                            } catch (IOException ex) {
                                logger.debug("", ex);
                            }
                        }
                    } catch (IOException e) {
                        logger.error("" + e);
                        if (bufReader != null) {
                            try {
                                bufReader.close();
                            } catch (IOException ex) {
                                logger.debug("", ex);
                            }
                        }
                    } finally {
                        if (bufReader != null) {
                            try {
                                bufReader.close();
                            } catch (IOException e) {
                                logger.debug("", e);
                            }
                        }
                    }
                    file.delete();
                    break;
                }
            }
        }
        return ret;
    }

    @Override
    protected void prepareSend()
            throws ConnectorException {
        prepareReceive();
    }

    @Override
    protected void internalSend(SimpleMessage message)
            throws ConnectorException {
        BufferedWriter output = null;
        try {
            File file = new File(directory + File.separator + message.getOriFileName() + "." + message.getSentMsgFormat());
            output = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), getEncoding()));
            output.write(message.getContent());
        } catch (IOException ex) {
            throw new ConnectorException("LocalFile internalSend failed", ex, true);
        } catch (Exception ex) {
            throw new ConnectorException("LocalFile internalSend failed", ex);
        } finally {
            try {
                if (output != null) {
                    output.close();
                }
            } catch (IOException localIOException1) {
            }
        }
    }

    @Override
    public String getConnType() {
        return "FILE";
    }
}



