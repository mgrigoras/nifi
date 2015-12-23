package org.apache.nifi.s2s;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.RemoteDestination;
import org.apache.nifi.remote.RemoteResourceInitiator;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.remote.client.socket.EndpointConnection;
import org.apache.nifi.remote.io.socket.SocketChannelCommunicationsSession;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.protocol.socket.SocketClientProtocol;
import org.apache.nifi.remote.util.StandardDataPacket;
import org.apache.nifi.util.NiFiProperties;
import org.springframework.util.Assert;

/**
 * A simple non-UI S2S sender based on the code extracted from EndpointConnectionPool.
 * Perhaps when https://issues.apache.org/jira/browse/NIFI-1331 is addressed this could serve
 * as a component to such pool for sending S2S
 *
 */
public class SiteToSiteSender implements AutoCloseable {

    private final SocketClientProtocol protocol;

    private final String s2sHost;

    private final int s2sPort;

    private final String s2sUrl;

    private volatile EndpointConnection connection;

    public SiteToSiteSender(final SiteToSiteClientConfig s2sConfig) {
        Assert.notNull(s2sConfig, "'s2sConfig' must not be null");
        this.s2sHost = NiFiProperties.getInstance().getRemoteInputHost();
        this.s2sPort = NiFiProperties.getInstance().getRemoteInputPort();
        this.s2sUrl = "nifi://" + this.s2sHost + ":" + this.s2sPort;
        this.protocol = new SocketClientProtocol();
        this.protocol.setEventReporter(s2sConfig.getEventReporter());
        this.protocol.setTimeout((int) s2sConfig.getTimeout(TimeUnit.MILLISECONDS));
        this.protocol.setDestination(new RemoteDestination() {
            @Override
            public boolean isUseCompression() {
                return false;
            }
            @Override
            public long getYieldPeriod(TimeUnit timeUnit) {
                return 10000;
            }
            @Override
            public String getName() {
                return s2sConfig.getPortName();
            }
            @Override
            public String getIdentifier() {
                return s2sConfig.getPortIdentifier();
            }
        });
    }

    public void send(byte[] flowFileContent, Map<String, String> attributes) {
        Assert.isTrue(flowFileContent != null && flowFileContent.length > 0,
                "'flowFileContent' must not be null or empty");
        Assert.notNull(attributes, "'attributes' must not be nul");

        synchronized (this.protocol) {
            if (this.connection == null) {
                this.connect();
            }
        }
        try {
            Transaction transaction = this.connection.getSocketClientProtocol().startTransaction(this.connection.getPeer(),
                    this.connection.getCodec(), TransferDirection.SEND);
            InputStream in = new ByteArrayInputStream(flowFileContent);
            DataPacket dataPacket = new StandardDataPacket(attributes, in, flowFileContent.length);
            transaction.send(dataPacket);
            transaction.confirm();
            transaction.complete();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to send data", e);
        }
    }

    @Override
    public void close() {

    }

    private void connect() {
        if (this.connection == null) {
            try {
                PeerDescription peerDesc = new PeerDescription(this.s2sHost, this.s2sPort, false);
                SocketChannel socketChannel = SocketChannel
                        .open(new InetSocketAddress(peerDesc.getHostname(), peerDesc.getPort()));
                CommunicationsSession session = new SocketChannelCommunicationsSession(socketChannel, this.s2sUrl);
                session.getOutput().getOutputStream().write(CommunicationsSession.MAGIC_BYTES);
                final DataInputStream dis = new DataInputStream(session.getInput().getInputStream());
                final DataOutputStream dos = new DataOutputStream(session.getOutput().getOutputStream());
                RemoteResourceInitiator.initiateResourceNegotiation(this.protocol, dis, dos);
                Peer peer = new Peer(peerDesc, session, this.s2sUrl, "http://localhost:8080/nifi");
                this.protocol.handshake(peer);
                this.connection = new EndpointConnection(peer, protocol, protocol.negotiateCodec(peer));
            } catch (Exception e) {
                throw new IllegalStateException("Failed to connect to " + this.s2sUrl, e);
            }
        }
    }
}
