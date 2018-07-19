package com.metinform.common.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Properties;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.metinform.common.BaseConnector;
import com.simple.datax.SimpleMessage;
import com.simple.datax.api.ConnectorException;

public class SFTPConn
        extends BaseConnector {
    private static final Logger logger = LoggerFactory.getLogger(SFTPConn.class);
    public static final String TYPE = "SFTP";
    private String username;
    private String host;
    private String port = "22";
    private String password;
    private ChannelSftp sftp;
    private String workingPath;
    private Session sshSession;
    private String trustMgr;
    private String knownHostFile;
    private long recvTotal;
    private long sendTotal;
    private static final String KNOWN_HOSTS = "../known_hosts";

    public SFTPConn(String connPara)
            throws ConnectorException {
        super(connPara);
        this.connPara = connPara;
        parseConnPara();
        init();
    }

    private void parseConnPara()
            throws ConnectorException {
        String[] tmpStr = splitConnPara(connPara);
        if (tmpStr.length == 7) {
            host = tmpStr[0];
            if ((tmpStr[1] != null) && (!tmpStr[1].trim().isEmpty())) {
                port = tmpStr[1];
            }
            username = tmpStr[2];
            password = tmpStr[3];
            workingPath = tmpStr[4];
            trustMgr = tmpStr[5];
            knownHostFile = tmpStr[6];
        } else {
            throw new ConnectorException("Parsing SFTPClient parameters failed. Please check parameter - " + connPara);
        }
    }

    private void init()
            throws ConnectorException {
        try {
            JSch jsch = new JSch();
            if (trustMgr.equalsIgnoreCase("YES")) {
                jsch.setKnownHosts("../known_hosts" + File.separator + knownHostFile);
            }
            sshSession = jsch.getSession(username, host, Integer.parseInt(port));
            sshSession.setPassword(password);

            Properties sshConfig = new Properties();
            if (trustMgr.equalsIgnoreCase("YES")) {
                sshConfig.put("StrictHostKeyChecking", "yes");
            } else {
                sshConfig.put("StrictHostKeyChecking", "no");
            }
            sshSession.setConfig(sshConfig);
            sshSession.connect();
            Channel channel = sshSession.openChannel("sftp");
            channel.connect();
            sftp = ((ChannelSftp) channel);
            changeWorkingDirectory(workingPath);
        } catch (Exception e) {
            disconnect();
            throw new ConnectorException("Init SFTP Connector failed - " + connPara, e);
        }
    }

    private void prepareSFTPClient()
            throws ConnectorException {
        if ((sshSession != null) && (sshSession.isConnected()) && (sftp != null) && (sftp.isConnected())) {
            return;
        }
        disconnect();
        sshSession = null;
        sftp = null;

        init();
    }

    @Override
    public String getMessage()
            throws ConnectorException {
        String ret = null;
        try {
            prepareSFTPClient();
            Vector<ChannelSftp.LsEntry> files = listFiles();
            if ((files != null) && (files.size() > 0)) {
                for (ChannelSftp.LsEntry entry : files) {
                    if (!entry.getAttrs().isDir()) {
                        ret = download(entry.getFilename());
                        if ((ret != null) && (ret.length() != 0)) {
                            delete(entry.getFilename());
                            logger.debug("########Receiver Get Message: #" + ++recvTotal + " - " + connPara);
                            break;
                        }
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("########Receiver Get Message Failed: #" + ++recvTotal + " - " + connPara);
            throw new ConnectorException("getMessage failed", ex);
        }
        return ret;
    }

    @Override
    protected void prepareSend()
            throws ConnectorException {
        prepareSFTPClient();
    }

    @Override
    protected void internalSend(SimpleMessage message)
            throws ConnectorException {
        try {
            upload(message.getOriFileName() + "." + message.getSentMsgFormat(), message.getContent());
            logger.debug("########Sender Sent Message: #" + ++sendTotal + " - " + connPara);
        } catch (SftpException ex) {
            logger.error("########Sender Sent Message Failed: #" + ++sendTotal + " - " + connPara);
            throw new ConnectorException("send message failed", ex, true);
        } catch (Exception ex) {
            logger.error("########Sender Sent Message Failed: #" + ++sendTotal + " - " + connPara);
            throw new ConnectorException("send message failed", ex);
        }
    }

    @Override
    public String getConnType() {
        return "SFTP";
    }

    private void upload(String remoteFile, String content)
            throws FileNotFoundException, SftpException, ConnectorException, UnsupportedEncodingException {
        if ((remoteFile == null) || (remoteFile.isEmpty()) || (content == null)) {
            throw new ConnectorException("Remote file name is null or content is null.");
        }
        ByteArrayInputStream input = null;
        try {
            input = new ByteArrayInputStream(content.getBytes(getEncoding()));
            sftp.put(input, remoteFile);
        } finally {
            try {
                input.close();
            } catch (IOException localIOException) {
            }
        }
    }

    private Vector<ChannelSftp.LsEntry> listFiles()
            throws SftpException {
        return sftp.ls(".");
    }

    private String download(String downloadFile)
            throws ConnectorException {
        String ret = null;
        ByteArrayOutputStream baos = null;
        try {
            baos = new ByteArrayOutputStream();
            sftp.get(downloadFile, baos);
            ret = baos.toString(getEncoding());
        } catch (Exception e) {
            throw new ConnectorException("Failed to get file. ", e);
        } finally {
            if (baos != null) {
                try {
                    baos.close();
                } catch (IOException localIOException) {
                }
            }
        }
        return ret;
    }

    public void delete(String deleteFile)
            throws ConnectorException {
        try {
            sftp.rm(deleteFile);
        } catch (Exception e) {
            throw new ConnectorException("Failed to download file. ", e);
        }
    }

    private void changeWorkingDirectory(String directory)
            throws ConnectorException {
        if ((workingPath == null) || (workingPath.trim().isEmpty())) {
            return;
        }
        try {
            sftp.cd(directory);
        } catch (Exception e) {
            throw new ConnectorException("Failed to change working directory - " + directory, e);
        }
    }

    @Override
    public void disconnect() {
        if (sshSession != null) {
            try {
                sshSession.disconnect();
            } catch (Exception ex) {
                logger.warn("sftp sshSession disconnect error", ex);
            }
        }
        if (sftp != null) {
            try {
                sftp.disconnect();
            } catch (Exception ex) {
                logger.warn("sftp disconnect error", ex);
            }
        }
        sshSession = null;
        sftp = null;
    }

    @Override
    protected void prepareReceive()
            throws ConnectorException {
        prepareSFTPClient();
    }
}



