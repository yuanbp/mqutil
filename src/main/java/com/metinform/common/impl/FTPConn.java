package com.metinform.common.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.KeyStore;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.net.PrintCommandListener;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.net.ftp.FTPSClient;
import org.apache.commons.net.util.TrustManagerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.metinform.common.BaseConnector;
import com.simple.datax.SimpleMessage;
import com.simple.datax.api.ConnectorException;

public class FTPConn
        extends BaseConnector {
    private static final Logger logger = LoggerFactory.getLogger(FTPConn.class);
    public static final String TYPE = "FTP";
    private FTPClient ftpClient = null;
    private String host;
    private int port = 21;
    private String username;
    private String password;
    private String workingPath = null;
    private boolean binaryTransfer = true;
    private boolean passiveMode = true;
    private String protocol = "NO";
    private String trustmgr = null;
    private String keymgr = null;
    private boolean bSSL = false;
    private String trust_path = null;
    private String key_path = null;
    private String key_pw = null;
    private String trust_pw = null;
    private boolean needMove = false;
    private String destDir = null;
    private String homeDir = null;
    ByteArrayOutputStream commandLog = new ByteArrayOutputStream();
    PrintWriter commandPrinter = new PrintWriter(commandLog);
    private static final String KEYSTORE_DIR = "../keystore/";

    public FTPConn(String connPara)
            throws ConnectorException {
        this(connPara, false);
    }

    public FTPConn(String connPara, boolean bSSL)
            throws ConnectorException {
        super(connPara);
        this.bSSL = bSSL;
        setConnPara(connPara);
        parseConnPara();
    }

    private void prepareFTPClient()
            throws ConnectorException {
        if ((ftpClient != null) && (ftpClient.isConnected())) {
            try {
                ftpClient.noop();
                return;
            } catch (Exception ex) {
                try {
                    ftpClient.disconnect();
                } catch (Exception e) {
                    logger.warn("FTP Client disconnect error:", e);
                }
                logger.debug("FTP Client needs re-init. ");
            }
        }
        createFTPClient();
        ftpClient.setControlEncoding(getEncoding());

        connect();
        if (passiveMode) {
            ftpClient.enterLocalPassiveMode();
        }
        setFileType();
        changeWorkingDirectory();
    }

    private void createFTPClient()
            throws ConnectorException {
        if (!bSSL) {
            ftpClient = new FTPClient();
        } else {
            FTPSClient ftps;
            if (protocol.equalsIgnoreCase("YES")) {
                ftps = new FTPSClient(true);
            } else {
                if (protocol.equalsIgnoreCase("NO")) {
                    ftps = new FTPSClient(false);
                } else {
                    ftps = new FTPSClient(false);
                }
            }
            ftps.setDefaultTimeout(10000);
            ftps.addProtocolCommandListener(new PrintCommandListener(commandPrinter));
            if ("all".equals(trustmgr)) {
                ftps.setTrustManager(
                        TrustManagerUtils.getAcceptAllTrustManager());
            } else if ("valid".equals(trustmgr)) {
                try {
                    ftps.setTrustManager(getTrustManager());
                } catch (Exception e) {
                    throw new ConnectorException("Set trust manager failed", e);
                }
            } else if ("none".equals(trustmgr)) {
                ftps.setTrustManager(null);
            }
            if ("valid".equals(keymgr)) {
                try {
                    ftps.setKeyManager(getKeyManager());
                } catch (Exception e) {
                    throw new ConnectorException("Set trust manager failed", e);
                }
            } else if ("none".equals(keymgr)) {
                ftps.setKeyManager(null);
            }
            ftpClient = ftps;
        }
    }

    private boolean connect()
            throws ConnectorException {
        try {
            ftpClient.connect(host, port);


            int reply = ftpClient.getReplyCode();
            if (FTPReply.isPositiveCompletion(reply)) {
                if (ftpClient.login(username, password)) {
                    homeDir = ftpClient.printWorkingDirectory();
                    return true;
                }
            } else {
                ftpClient.disconnect();
                throw new ConnectorException("FTP server refused connection.");
            }
        } catch (Exception e) {
            if (ftpClient.isConnected()) {
                try {
                    ftpClient.disconnect();
                } catch (IOException e1) {
                    logger.debug("Connector login failed and disconnect it failed.");
                }
            }
            throw new ConnectorException("Could not connect to server.", e);
        } finally {
            logger.debug(commandLog.toString());
        }
        logger.debug(commandLog.toString());

        return false;
    }

    @Override
    public void disconnect() {
        try {
            ftpClient.logout();
            if (ftpClient.isConnected()) {
                ftpClient.disconnect();
                ftpClient = null;
            }
        } catch (IOException e) {
            logger.warn("Could not disconnect from server.", e);
        }
    }

    private void setFileType()
            throws ConnectorException {
        try {
            if (binaryTransfer) {
                ftpClient.setFileType(2);
            } else {
                ftpClient.setFileType(0);
            }
        } catch (IOException e) {
            throw new ConnectorException("Could not to set file type.", e);
        }
    }

    @Override
    public String getMessage()
            throws ConnectorException {
        String ret = null;
        try {
            prepareFTPClient();
            FTPFile[] files = (FTPFile[]) null;
            try {
                files = ftpClient.listFiles();
            } catch (Exception e1) {
                throw new ConnectorException("Going through directory failed.", e1);
            }
            if ((files != null) && (files.length > 0)) {
                label190:
                for (FTPFile file : files) {
                    if (file.isFile()) {
                        String name = file.getName();
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        try {
                            ftpClient.retrieveFile(name, baos);
                            getReplyCode();
                            ret = baos.toString(getEncoding());
                            if ((ret == null) || (ret.length() == 0)) {
                                if (baos == null) {
                                    continue;
                                }
                                try {
                                    baos.close();
                                } catch (IOException localIOException1) {
                                }
                            }
                            if (baos == null) {
                                break label190;
                            }
                        } catch (IOException e) {
                            throw new ConnectorException("Get remote file failed", e);
                        } finally {
                            if (baos != null) {
                                try {
                                    baos.close();
                                } catch (IOException localIOException2) {
                                }
                            }
                        }
                        try {
                            baos.close();
                        } catch (IOException localIOException3) {
                        }
                        try {
                            if (needMove) {
                                move(name, homeDir + File.separator + destDir + File.separator + name);
                            } else {
                                ftpClient.deleteFile(name);
                            }
                            getReplyCode();
                        } catch (IOException e) {
                            throw new ConnectorException("Can't delete file - " +
                                    name, e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new ConnectorException("Get remote file failed", e);
        }
        return ret;
    }

    private void parseConnPara()
            throws ConnectorException {
        String[] tmpStr = splitConnPara(connPara);
        if (tmpStr.length == 14) {
            host = tmpStr[0];
            try {
                port = Integer.parseInt(tmpStr[1]);
            } catch (NumberFormatException e) {
                logger.error("Connector port is invalid." + tmpStr[1], e);
            }
            username = tmpStr[2];
            password = tmpStr[3];
            workingPath = tmpStr[4];
            protocol = tmpStr[5];
            keymgr = tmpStr[6];
            trustmgr = tmpStr[7];
            key_path = tmpStr[8];
            trust_path = tmpStr[9];
            key_pw = tmpStr[10];
            trust_pw = tmpStr[11];
            if ("YES".equalsIgnoreCase(tmpStr[12])) {
                needMove = true;
            }
            destDir = tmpStr[13];
        } else if (tmpStr.length == 7) {
            host = tmpStr[0];
            port = Integer.parseInt(tmpStr[1]);
            username = tmpStr[2];
            password = tmpStr[3];
            workingPath = tmpStr[4];
            if ("YES".equalsIgnoreCase(tmpStr[5])) {
                needMove = true;
            }
            destDir = tmpStr[6];
        } else {
            throw new ConnectorException("Parsing FTPClient parameters failed. Please check parameter - " + connPara);
        }
    }

    @Override
    public String getConnType() {
        if (bSSL) {
            return "FTPS";
        }
        return "FTP";
    }

    private void put(String remoteAbsoluteFile, String content)
            throws ConnectorException, IOException {
        ByteArrayInputStream input = null;
        boolean ret = false;
        if ((remoteAbsoluteFile == null) || (remoteAbsoluteFile.isEmpty()) || (content == null)) {
            throw new ConnectorException("Remote file name is null or content is null.");
        }
        try {
            input = new ByteArrayInputStream(content.getBytes(getEncoding()));
            ret = ftpClient.storeFile(remoteAbsoluteFile, input);
            if (!ret) {
                throw new ConnectorException("Uploading File Failed. Reply Code:" + ftpClient.getReplyCode() + ". Please check error log for detail reply.");
            }
            getReplyCode();
        } catch (FileNotFoundException e) {
            throw e;
        } catch (IOException e) {
            throw e;
        } finally {
            try {
                if (input != null) {
                    input.close();
                }
            } catch (Exception e) {
                logger.error("Close file input stream failed.", e);
            }
        }
    }

    private void move(String srcAbsoluteFile, String destAbsoluteFile)
            throws ConnectorException, IOException {
        boolean ret = false;
        ret = ftpClient.rename(srcAbsoluteFile, destAbsoluteFile);
        getReplyCode();
        if (!ret) {
            throw new ConnectorException("Moving File Failed. Reply Code:" + ftpClient.getReplyCode() + ". Please check error log for detail reply.");
        }
    }

    private void changeWorkingDirectory()
            throws ConnectorException {
        boolean success = false;
        try {
            if ((workingPath == null) || (workingPath.trim().isEmpty())) {
                success = true;
            } else {
                success = ftpClient.changeWorkingDirectory(workingPath);
            }
        } catch (IOException ex) {
            throw new ConnectorException("Changing working directory failed", ex);
        }
        int reply = ftpClient.getReplyCode();
        if (!FTPReply.isPositiveCompletion(reply)) {
            logger.debug("FTP reply is " + reply);
        }
        if (!success) {
            throw new ConnectorException("Failed to change working directory. See server's reply.");
        }
    }

    private void getReplyCode() {
        int reply = ftpClient.getReplyCode();
        if (!FTPReply.isPositiveCompletion(reply)) {
            logger.error("Reply Code: " + reply);
        }
    }

    static int fileCountName = 2;

    @Override
    protected void prepareSend()
            throws ConnectorException {
        prepareFTPClient();
    }

    @Override
    protected void internalSend(SimpleMessage message)
            throws ConnectorException {
        try {
            String fileName = message.getOriFileName() + "." + message.getSentMsgFormat();
            put(fileName, message.getContent());
        } catch (IOException ex) {
            throw new ConnectorException("FTP internalSend failed", ex, true);
        } catch (Exception ex) {
            throw new ConnectorException("FTP internalSend failed", ex);
        }
    }

    private KeyManager getKeyManager()
            throws Exception {
        String keyfile = "../keystore/" + key_path;
        KeyStore key_ks = KeyStore.getInstance("JKS");
        key_ks.load(new FileInputStream(keyfile), key_pw.toCharArray());

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(key_ks, key_pw.toCharArray());
        KeyManager[] km = kmf.getKeyManagers();

        return km[0];
    }

    private TrustManager getTrustManager()
            throws Exception {
        String keyfile = "../keystore/" + trust_path;
        KeyStore trust_ks = KeyStore.getInstance("JKS");
        trust_ks.load(new FileInputStream(keyfile), trust_pw.toCharArray());

        TrustManagerFactory tf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tf.init(trust_ks);

        TrustManager[] tm = tf.getTrustManagers();

        return tm[0];
    }

    @Override
    protected void prepareReceive()
            throws ConnectorException {
        prepareFTPClient();
    }
}



