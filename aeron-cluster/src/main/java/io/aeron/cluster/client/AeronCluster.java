/*
 * Copyright 2014-2018 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.cluster.client;

import io.aeron.*;
import io.aeron.cluster.codecs.*;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.exceptions.TimeoutException;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.Header;
import io.aeron.security.AuthenticationException;
import io.aeron.security.CredentialsSupplier;
import io.aeron.security.NullCredentialsSupplier;
import org.agrona.AsciiEncoding;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.*;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.aeron.cluster.client.SessionDecorator.SESSION_HEADER_LENGTH;
import static org.agrona.SystemUtil.getDurationInNanos;

/**
 * Client for interacting with an Aeron Cluster.
 * <p>
 * A client will connect to open a session and then offer ingress messages which are replicated to clustered service
 * for reliability. If the clustered service responds then these response messages and events come back via the egress
 * stream.
 * <p>
 * <b>Note:</b> Instances of this class are not threadsafe.
 */
public final class AeronCluster implements AutoCloseable
{
    private static final int SEND_ATTEMPTS = 3;
    private static final int CONNECT_FRAGMENT_LIMIT = 1;
    private static final int SESSION_FRAGMENT_LIMIT = 10;

    private long lastCorrelationId = Aeron.NULL_VALUE;
    private final long clusterSessionId;
    private int leaderMemberId = Aeron.NULL_VALUE;
    private final boolean isUnicast;
    private final Context ctx;
    private final Aeron aeron;
    private final Subscription subscription;
    private Publication publication;
    private final NanoClock nanoClock;
    private final IdleStrategy idleStrategy;

    private final Int2ObjectHashMap<String> endpointByMemberIdMap = new Int2ObjectHashMap<>();
    private final BufferClaim bufferClaim = new BufferClaim();
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final SessionKeepAliveRequestEncoder keepAliveRequestEncoder = new SessionKeepAliveRequestEncoder();
    private final SessionHeaderEncoder sessionHeaderEncoder = new SessionHeaderEncoder();
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SessionHeaderDecoder sessionHeaderDecoder = new SessionHeaderDecoder();
    private final NewLeaderEventDecoder newLeaderEventDecoder = new NewLeaderEventDecoder();
    private final DirectBufferVector[] vectors = new DirectBufferVector[2];
    private final DirectBufferVector messageBuffer = new DirectBufferVector();
    private final FragmentAssembler fragmentAssembler = new FragmentAssembler(this::onFragment, 0, true);
    private final SessionMessageListener sessionMessageListener;

    /**
     * Connect to the cluster using default configuration.
     *
     * @return allocated cluster client if the connection is successful.
     */
    public static AeronCluster connect()
    {
        return connect(new Context());
    }

    /**
     * Connect to the cluster providing {@link Context} for configuration.
     *
     * @param ctx for configuration.
     * @return allocated cluster client if the connection is successful.
     */
    public static AeronCluster connect(final AeronCluster.Context ctx)
    {
        return new AeronCluster(ctx);
    }

    private AeronCluster(final Context ctx)
    {
        this.ctx = ctx;

        Subscription subscription = null;

        try
        {
            ctx.conclude();

            this.aeron = ctx.aeron();
            this.idleStrategy = ctx.idleStrategy();
            this.nanoClock = aeron.context().nanoClock();
            this.isUnicast = ctx.clusterMemberEndpoints() != null;
            this.sessionMessageListener = ctx.sessionMessageListener();
            updateMemberEndpoints(ctx.clusterMemberEndpoints());

            subscription = aeron.addSubscription(ctx.egressChannel(), ctx.egressStreamId());
            this.subscription = subscription;

            publication = connectToCluster();
            this.clusterSessionId = openSession();

            final UnsafeBuffer headerBuffer = new UnsafeBuffer(new byte[SESSION_HEADER_LENGTH]);
            sessionHeaderEncoder
                .wrapAndApplyHeader(headerBuffer, 0, messageHeaderEncoder)
                .clusterSessionId(clusterSessionId)
                .timestamp(Aeron.NULL_VALUE);

            vectors[0] = new DirectBufferVector(headerBuffer, 0, SESSION_HEADER_LENGTH);
            vectors[1] = messageBuffer;
        }
        catch (final Exception ex)
        {
            if (!ctx.ownsAeronClient())
            {
                CloseHelper.quietClose(publication);
                CloseHelper.quietClose(subscription);
            }

            CloseHelper.quietClose(ctx);
            throw ex;
        }
    }

    /**
     * Close session and release associated resources.
     */
    public void close()
    {
        if (null != publication && publication.isConnected())
        {
            closeSession();
        }

        if (!ctx.ownsAeronClient())
        {
            CloseHelper.close(subscription);
            CloseHelper.close(publication);
        }

        ctx.close();
    }

    /**
     * Get the context used to launch this cluster client.
     *
     * @return the context used to launch this cluster client.
     */
    public Context context()
    {
        return ctx;
    }

    /**
     * Cluster session id for the session that was opened as the result of a successful connect.
     *
     * @return session id for the session that was opened as the result of a successful connect.
     */
    public long clusterSessionId()
    {
        return clusterSessionId;
    }

    /**
     * Get the current leader member id for the cluster.
     *
     * @return the current leader member id for the cluster.
     */
    public int leaderMemberId()
    {
        return leaderMemberId;
    }

    /**
     * Get the last correlation id generated for this session. Starts with {@link Aeron#NULL_VALUE}.
     *
     * @return the last correlation id generated for this session.
     * @see #nextCorrelationId()
     */
    public long lastCorrelationId()
    {
        return lastCorrelationId;
    }

    /**
     * Generate a new correlation id to be used for this session. This is not threadsafe. If you require a threadsafe
     * correlation id generation then use {@link Aeron#nextCorrelationId()}.
     *
     * @return a new correlation id to be used for this session.
     * @see #lastCorrelationId()
     */
    public long nextCorrelationId()
    {
        return ++lastCorrelationId;
    }

    /**
     * Get the raw {@link Publication} for sending to the cluster.
     * <p>
     * This can be wrapped with a {@link SessionDecorator} for pre-pending the cluster session header to messages.
     * {@link io.aeron.cluster.codecs.SessionHeaderEncoder} or equivalent should be used to raw access.
     *
     * @return the raw {@link Publication} for connecting to the cluster.
     */
    public Publication ingressPublication()
    {
        return publication;
    }

    /**
     * Get the raw {@link Subscription} for receiving from the cluster.
     * <p>
     * The can be wrapped with a {@link EgressAdapter} for dispatching events from the cluster.
     *
     * @return the raw {@link Subscription} for receiving from the cluster.
     */
    public Subscription egressSubscription()
    {
        return subscription;
    }

    /**
     * Non-blocking publish of a partial buffer containing a message plus session header to a cluster.
     * <p>
     * This version of the method will set the timestamp value in the header to zero.
     *
     * @param correlationId to be used to identify the message to the cluster.
     * @param buffer        containing message.
     * @param offset        offset in the buffer at which the encoded message begins.
     * @param length        in bytes of the encoded message.
     * @return the same as {@link Publication#offer(DirectBuffer, int, int)}.
     */
    public long offer(final long correlationId, final DirectBuffer buffer, final int offset, final int length)
    {
        sessionHeaderEncoder.correlationId(correlationId);
        messageBuffer.reset(buffer, offset, length);

        return publication.offer(vectors, null);
    }

    /**
     * Send a keep alive message to the cluster to keep this session open.
     *
     * @return true if successfully sent otherwise false.
     */
    public boolean sendKeepAlive()
    {
        idleStrategy.reset();
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + SessionKeepAliveRequestEncoder.BLOCK_LENGTH;
        int attempts = SEND_ATTEMPTS;

        while (true)
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                keepAliveRequestEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .correlationId(Aeron.NULL_VALUE)
                    .clusterSessionId(clusterSessionId);

                bufferClaim.commit();

                return true;
            }

            checkResult(result);

            if (--attempts <= 0)
            {
                break;
            }

            idleStrategy.idle();
        }

        return false;
    }

    /**
     * Poll the {@link #egressSubscription()} for session messages which are dispatched to
     * {@link Context#sessionMessageListener()}.
     * <p>
     * <b>Note:</b> if {@link Context#sessionMessageListener()} is not set then a {@link ConfigurationException} could
     * result.
     *
     * @return the number of fragments processed.
     */
    public int pollEgress()
    {
        return subscription.poll(fragmentAssembler, SESSION_FRAGMENT_LIMIT);
    }

    /**
     * To be called when a new leader event is delivered. This method needs to be called when using the
     * {@link EgressAdapter} or {@link EgressPoller} rather than {@link #pollEgress()} method.
     *
     * @param clusterSessionId which must match {@link #clusterSessionId()}.
     * @param leaderMemberId   which has become the new leader.
     * @param memberEndpoints  comma separated list of cluster members endpoints to connect to with the leader first.
     */
    public void onNewLeader(final long clusterSessionId, final int leaderMemberId, final String memberEndpoints)
    {
        if (clusterSessionId != this.clusterSessionId)
        {
            throw new ClusterException(
                "invalid clusterSessionId=" + clusterSessionId + " expected " + this.clusterSessionId);
        }

        this.leaderMemberId = leaderMemberId;

        if (isUnicast)
        {
            CloseHelper.close(publication);
            fragmentAssembler.clear();
            updateMemberEndpoints(memberEndpoints);

            final ChannelUri channelUri = ChannelUri.parse(ctx.ingressChannel());
            channelUri.put(CommonContext.ENDPOINT_PARAM_NAME, endpointByMemberIdMap.get(leaderMemberId));
            publication = addIngressPublication(channelUri.toString(), ctx.ingressStreamId());
        }
    }

    private void updateMemberEndpoints(final String memberEndpoints)
    {
        endpointByMemberIdMap.clear();

        if (null != memberEndpoints)
        {
            for (final String member : memberEndpoints.split(","))
            {
                final int separatorIndex = member.indexOf('=');
                if (-1 == separatorIndex)
                {
                    throw new ConfigurationException(
                        "invalid format - member missing '=' separator: " + memberEndpoints);
                }

                final int memberId = AsciiEncoding.parseIntAscii(member, 0, separatorIndex);
                endpointByMemberIdMap.put(memberId, member.substring(separatorIndex + 1));
            }
        }
    }

    private void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int templateId = messageHeaderDecoder.templateId();
        if (SessionHeaderDecoder.TEMPLATE_ID == templateId)
        {
            sessionHeaderDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

            final long sessionId = sessionHeaderDecoder.clusterSessionId();
            if (sessionId == clusterSessionId)
            {
                sessionMessageListener.onMessage(
                    sessionHeaderDecoder.correlationId(),
                    sessionId,
                    sessionHeaderDecoder.timestamp(),
                    buffer,
                    offset + SESSION_HEADER_LENGTH,
                    length - SESSION_HEADER_LENGTH,
                    header);
            }
        }
        else if (NewLeaderEventDecoder.TEMPLATE_ID == templateId)
        {
            newLeaderEventDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

            final long sessionId = newLeaderEventDecoder.clusterSessionId();
            if (sessionId == clusterSessionId)
            {
                onNewLeader(sessionId, newLeaderEventDecoder.leaderMemberId(), newLeaderEventDecoder.memberEndpoints());
            }
        }
    }

    private void closeSession()
    {
        idleStrategy.reset();
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + SessionCloseRequestEncoder.BLOCK_LENGTH;
        final SessionCloseRequestEncoder sessionCloseRequestEncoder = new SessionCloseRequestEncoder();
        int attempts = SEND_ATTEMPTS;

        while (true)
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                sessionCloseRequestEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .clusterSessionId(clusterSessionId);

                bufferClaim.commit();
                break;
            }

            checkResult(result);

            if (--attempts <= 0)
            {
                break;
            }

            idleStrategy.idle();
        }
    }

    private Publication connectToCluster()
    {
        Publication publication = null;
        final String ingressChannel = ctx.ingressChannel();
        final int ingressStreamId = ctx.ingressStreamId();
        final long deadlineNs = nanoClock.nanoTime() + ctx.messageTimeoutNs();

        if (isUnicast)
        {
            final ChannelUri channelUri = ChannelUri.parse(ingressChannel);
            final int memberCount = endpointByMemberIdMap.size();
            final Publication[] publications = new Publication[memberCount];

            for (final Map.Entry<Integer, String> entry : endpointByMemberIdMap.entrySet())
            {
                channelUri.put(CommonContext.ENDPOINT_PARAM_NAME, entry.getValue());
                final String channel = channelUri.toString();
                publications[entry.getKey()] = addIngressPublication(channel, ingressStreamId);
            }

            int connectedIndex = -1;
            while (true)
            {
                for (int i = 0; i < memberCount; i++)
                {
                    if (publications[i].isConnected())
                    {
                        connectedIndex = i;
                        break;
                    }
                }

                if (-1 != connectedIndex)
                {
                    for (int i = 0; i < memberCount; i++)
                    {
                        if (i == connectedIndex)
                        {
                            publication = publications[i];
                        }
                        else
                        {
                            CloseHelper.close(publications[i]);
                        }
                    }

                    break;
                }

                if (nanoClock.nanoTime() > deadlineNs)
                {
                    for (int i = 0; i < memberCount; i++)
                    {
                        CloseHelper.quietClose(publications[i]);
                    }

                    throw new TimeoutException("awaiting connection to cluster");
                }

                idleStrategy.idle();
            }
        }
        else
        {
            publication = addIngressPublication(ingressChannel, ingressStreamId);

            idleStrategy.reset();
            while (!publication.isConnected())
            {
                if (nanoClock.nanoTime() > deadlineNs)
                {
                    CloseHelper.quietClose(publication);
                    throw new TimeoutException("awaiting connection to cluster");
                }

                idleStrategy.idle();
            }
        }

        return publication;
    }

    private Publication addIngressPublication(final String channel, final int streamId)
    {
        if (ctx.isIngressExclusive())
        {
            return aeron.addExclusivePublication(channel, streamId);
        }
        else
        {
            return aeron.addPublication(channel, streamId);
        }
    }

    private long openSession()
    {
        final long deadlineNs = nanoClock.nanoTime() + ctx.messageTimeoutNs();
        long correlationId = sendConnectRequest(ctx.credentialsSupplier().encodedCredentials(), deadlineNs);
        final EgressPoller poller = new EgressPoller(subscription, CONNECT_FRAGMENT_LIMIT);

        while (true)
        {
            pollNextResponse(deadlineNs, correlationId, poller);

            if (poller.correlationId() == correlationId)
            {
                if (poller.isChallenged())
                {
                    final byte[] encodedCredentials = ctx.credentialsSupplier().onChallenge(poller.encodedChallenge());
                    correlationId = sendChallengeResponse(poller.clusterSessionId(), encodedCredentials, deadlineNs);
                    continue;
                }

                switch (poller.eventCode())
                {
                    case OK:
                        this.leaderMemberId = poller.leaderMemberId();
                        return poller.clusterSessionId();

                    case ERROR:
                        throw new AuthenticationException(poller.detail());

                    case AUTHENTICATION_REJECTED:
                        throw new AuthenticationException(poller.detail());
                }
            }
        }
    }

    private void pollNextResponse(final long deadlineNs, final long correlationId, final EgressPoller poller)
    {
        idleStrategy.reset();

        while (poller.poll() <= 0 && !poller.isPollComplete())
        {
            if (nanoClock.nanoTime() > deadlineNs)
            {
                throw new TimeoutException("awaiting response for correlationId=" + correlationId);
            }

            idleStrategy.idle();
        }
    }

    private long sendConnectRequest(final byte[] encodedCredentials, final long deadlineNs)
    {
        lastCorrelationId = aeron.nextCorrelationId();

        final SessionConnectRequestEncoder sessionConnectRequestEncoder = new SessionConnectRequestEncoder();
        final int length = MessageHeaderEncoder.ENCODED_LENGTH +
            SessionConnectRequestEncoder.BLOCK_LENGTH +
            SessionConnectRequestEncoder.responseChannelHeaderLength() +
            ctx.egressChannel().length() +
            SessionConnectRequestEncoder.encodedCredentialsHeaderLength() +
            encodedCredentials.length;

        idleStrategy.reset();

        while (true)
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                sessionConnectRequestEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .correlationId(lastCorrelationId)
                    .responseStreamId(ctx.egressStreamId())
                    .responseChannel(ctx.egressChannel())
                    .putEncodedCredentials(encodedCredentials, 0, encodedCredentials.length);

                bufferClaim.commit();

                break;
            }

            if (Publication.CLOSED == result)
            {
                throw new ClusterException("unexpected close from cluster");
            }

            if (nanoClock.nanoTime() > deadlineNs)
            {
                throw new TimeoutException("failed to connect to cluster");
            }

            idleStrategy.idle();
        }

        return lastCorrelationId;
    }

    private long sendChallengeResponse(final long sessionId, final byte[] encodedCredentials, final long deadlineNs)
    {
        lastCorrelationId = aeron.nextCorrelationId();

        final ChallengeResponseEncoder challengeResponseEncoder = new ChallengeResponseEncoder();
        final int length = MessageHeaderEncoder.ENCODED_LENGTH +
            ChallengeResponseEncoder.BLOCK_LENGTH +
            ChallengeResponseEncoder.encodedCredentialsHeaderLength() +
            encodedCredentials.length;

        idleStrategy.reset();

        while (true)
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                challengeResponseEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .correlationId(lastCorrelationId)
                    .clusterSessionId(sessionId)
                    .putEncodedCredentials(encodedCredentials, 0, encodedCredentials.length);

                bufferClaim.commit();

                break;
            }

            checkResult(result);

            if (nanoClock.nanoTime() > deadlineNs)
            {
                throw new TimeoutException("failed to connect to cluster");
            }

            idleStrategy.idle();
        }

        return lastCorrelationId;
    }

    private static void checkResult(final long result)
    {
        if (result == Publication.NOT_CONNECTED ||
            result == Publication.CLOSED ||
            result == Publication.MAX_POSITION_EXCEEDED)
        {
            throw new ClusterException("unexpected publication state: " + result);
        }
    }

    /**
     * Configuration options for cluster client.
     */
    public static class Configuration
    {
        /**
         * Timeout when waiting on a message to be sent or received.
         */
        public static final String MESSAGE_TIMEOUT_PROP_NAME = "aeron.cluster.message.timeout";

        /**
         * Default timeout when waiting on a message to be sent or received.
         */
        public static final long MESSAGE_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(5);

        /**
         * Property name for the comma separated list of cluster member endpoints for use with unicast. This is the
         * endpoint values which get substituted into the {@link #INGRESS_CHANNEL_PROP_NAME} when using UDP unicast.
         * <p>
         * {@code "0=endpoint,1=endpoint,2=endpoint"}
         * <p>
         * Each member of the list will be substituted for the endpoint in the {@link #INGRESS_CHANNEL_PROP_NAME} value.
         */
        public static final String CLUSTER_MEMBER_ENDPOINTS_PROP_NAME = "aeron.cluster.member.endpoints";

        /**
         * Default comma separated list of cluster member endpoints.
         */
        public static final String CLUSTER_MEMBER_ENDPOINTS_DEFAULT = null;

        /**
         * Channel for sending messages to a cluster. Ideally this will be a multicast address otherwise unicast will
         * be required and the {@link #CLUSTER_MEMBER_ENDPOINTS_PROP_NAME} is used to substitute the endpoints from
         * the {@link #CLUSTER_MEMBER_ENDPOINTS_PROP_NAME} list.
         */
        public static final String INGRESS_CHANNEL_PROP_NAME = "aeron.cluster.ingress.channel";

        /**
         * Channel for sending messages to a cluster.
         */
        public static final String INGRESS_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:9010";

        /**
         * Stream id within a channel for sending messages to a cluster.
         */
        public static final String INGRESS_STREAM_ID_PROP_NAME = "aeron.cluster.ingress.stream.id";

        /**
         * Default stream id within a channel for sending messages to a cluster.
         */
        public static final int INGRESS_STREAM_ID_DEFAULT = 101;

        /**
         * Channel for receiving response messages from a cluster.
         */
        public static final String EGRESS_CHANNEL_PROP_NAME = "aeron.cluster.egress.channel";

        /**
         * Channel for receiving response messages from a cluster.
         */
        public static final String EGRESS_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:9020";

        /**
         * Stream id within a channel for receiving messages from a cluster.
         */
        public static final String EGRESS_STREAM_ID_PROP_NAME = "aeron.archive.control.response.stream.id";

        /**
         * Default stream id within a channel for receiving messages from a cluster.
         */
        public static final int EGRESS_STREAM_ID_DEFAULT = 102;

        /**
         * The timeout in nanoseconds to wait for a message.
         *
         * @return timeout in nanoseconds to wait for a message.
         * @see #MESSAGE_TIMEOUT_PROP_NAME
         */
        public static long messageTimeoutNs()
        {
            return getDurationInNanos(MESSAGE_TIMEOUT_PROP_NAME, MESSAGE_TIMEOUT_DEFAULT_NS);
        }

        /**
         * The value {@link #CLUSTER_MEMBER_ENDPOINTS_DEFAULT} or system property
         * {@link #CLUSTER_MEMBER_ENDPOINTS_PROP_NAME} if set.
         *
         * @return {@link #CLUSTER_MEMBER_ENDPOINTS_DEFAULT} or system property
         * {@link #CLUSTER_MEMBER_ENDPOINTS_PROP_NAME} if set.
         */
        public static String clusterMemberEndpoints()
        {
            return System.getProperty(
                CLUSTER_MEMBER_ENDPOINTS_PROP_NAME, CLUSTER_MEMBER_ENDPOINTS_DEFAULT);
        }

        /**
         * The value {@link #INGRESS_CHANNEL_DEFAULT} or system property
         * {@link #INGRESS_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #INGRESS_CHANNEL_DEFAULT} or system property
         * {@link #INGRESS_CHANNEL_PROP_NAME} if set.
         */
        public static String ingressChannel()
        {
            return System.getProperty(INGRESS_CHANNEL_PROP_NAME, INGRESS_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #INGRESS_STREAM_ID_DEFAULT} or system property
         * {@link #INGRESS_STREAM_ID_PROP_NAME} if set.
         *
         * @return {@link #INGRESS_STREAM_ID_DEFAULT} or system property
         * {@link #INGRESS_STREAM_ID_PROP_NAME} if set.
         */
        public static int ingressStreamId()
        {
            return Integer.getInteger(INGRESS_STREAM_ID_PROP_NAME, INGRESS_STREAM_ID_DEFAULT);
        }

        /**
         * The value {@link #EGRESS_CHANNEL_DEFAULT} or system property
         * {@link #EGRESS_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #EGRESS_CHANNEL_DEFAULT} or system property
         * {@link #EGRESS_CHANNEL_PROP_NAME} if set.
         */
        public static String egressChannel()
        {
            return System.getProperty(EGRESS_CHANNEL_PROP_NAME, EGRESS_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #EGRESS_STREAM_ID_DEFAULT} or system property
         * {@link #EGRESS_STREAM_ID_PROP_NAME} if set.
         *
         * @return {@link #EGRESS_STREAM_ID_DEFAULT} or system property
         * {@link #EGRESS_STREAM_ID_PROP_NAME} if set.
         */
        public static int egressStreamId()
        {
            return Integer.getInteger(EGRESS_STREAM_ID_PROP_NAME, EGRESS_STREAM_ID_DEFAULT);
        }
    }

    /**
     * Context for cluster session and connection.
     */
    public static class Context implements AutoCloseable, Cloneable
    {
        private long messageTimeoutNs = Configuration.messageTimeoutNs();
        private String clusterMemberEndpoints = Configuration.clusterMemberEndpoints();
        private String ingressChannel = Configuration.ingressChannel();
        private int ingressStreamId = Configuration.ingressStreamId();
        private String egressChannel = Configuration.egressChannel();
        private int egressStreamId = Configuration.egressStreamId();
        private IdleStrategy idleStrategy;
        private String aeronDirectoryName = CommonContext.getAeronDirectoryName();
        private Aeron aeron;
        private CredentialsSupplier credentialsSupplier;
        private boolean ownsAeronClient = false;
        private boolean isIngressExclusive = true;
        private ErrorHandler errorHandler = Aeron.DEFAULT_ERROR_HANDLER;
        private SessionMessageListener sessionMessageListener;

        /**
         * Perform a shallow copy of the object.
         *
         * @return a shallow copy of the object.
         */
        public Context clone()
        {
            try
            {
                return (Context)super.clone();
            }
            catch (final CloneNotSupportedException ex)
            {
                throw new RuntimeException(ex);
            }
        }

        public void conclude()
        {
            if (null == aeron)
            {
                aeron = Aeron.connect(
                    new Aeron.Context()
                        .aeronDirectoryName(aeronDirectoryName)
                        .errorHandler(errorHandler));

                ownsAeronClient = true;
            }

            if (null == idleStrategy)
            {
                idleStrategy = new BackoffIdleStrategy(1, 10, 1000, 1000);
            }

            if (null == credentialsSupplier)
            {
                credentialsSupplier = new NullCredentialsSupplier();
            }

            if (null == sessionMessageListener)
            {
                sessionMessageListener =
                    (correlationId, clusterSessionId, timestamp, buffer, offset, length, header) ->
                    {
                        throw new ConfigurationException(
                            "sessionMessageListener must be specified on AeronCluster.Context");
                    };
            }
        }

        /**
         * Set the message timeout in nanoseconds to wait for sending or receiving a message.
         *
         * @param messageTimeoutNs to wait for sending or receiving a message.
         * @return this for a fluent API.
         * @see Configuration#MESSAGE_TIMEOUT_PROP_NAME
         */
        public Context messageTimeoutNs(final long messageTimeoutNs)
        {
            this.messageTimeoutNs = messageTimeoutNs;
            return this;
        }

        /**
         * The message timeout in nanoseconds to wait for sending or receiving a message.
         *
         * @return the message timeout in nanoseconds to wait for sending or receiving a message.
         * @see Configuration#MESSAGE_TIMEOUT_PROP_NAME
         */
        public long messageTimeoutNs()
        {
            return messageTimeoutNs;
        }

        /**
         * The endpoints representing members for use with unicast to be substituted into the {@link #ingressChannel()}
         * for endpoints. A null value can be used when multicast where the {@link #ingressChannel()} contains the
         * multicast endpoint.
         *
         * @param clusterMembers which are all candidates to be leader.
         * @return this for a fluent API.
         * @see Configuration#CLUSTER_MEMBER_ENDPOINTS_PROP_NAME
         */
        public Context clusterMemberEndpoints(final String clusterMembers)
        {
            this.clusterMemberEndpoints = clusterMembers;
            return this;
        }

        /**
         * The endpoints representing members for use with unicast to be substituted into the {@link #ingressChannel()}
         * for endpoints. A null value can be used when multicast where the {@link #ingressChannel()} contains the
         * multicast endpoint.
         *
         * @return members of the cluster which are all candidates to be leader.
         * @see Configuration#CLUSTER_MEMBER_ENDPOINTS_PROP_NAME
         */
        public String clusterMemberEndpoints()
        {
            return clusterMemberEndpoints;
        }

        /**
         * Set the channel parameter for the ingress channel.
         * <p>
         * The endpoints representing members for use with unicast are substituted from the
         * {@link #clusterMemberEndpoints()} for endpoints. A null value can be used when multicast
         * where this contains the multicast endpoint.
         *
         * @param channel parameter for the ingress channel.
         * @return this for a fluent API.
         * @see Configuration#INGRESS_CHANNEL_PROP_NAME
         */
        public Context ingressChannel(final String channel)
        {
            ingressChannel = channel;
            return this;
        }

        /**
         * Get the channel parameter for the ingress channel.
         * <p>
         * The endpoints representing members for use with unicast are substituted from the
         * {@link #clusterMemberEndpoints()} for endpoints. A null value can be used when multicast
         * where this contains the multicast endpoint.
         *
         * @return the channel parameter for the ingress channel.
         * @see Configuration#INGRESS_CHANNEL_PROP_NAME
         */
        public String ingressChannel()
        {
            return ingressChannel;
        }

        /**
         * Set the stream id for the ingress channel.
         *
         * @param streamId for the ingress channel.
         * @return this for a fluent API
         * @see Configuration#INGRESS_STREAM_ID_PROP_NAME
         */
        public Context ingressStreamId(final int streamId)
        {
            ingressStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for the ingress channel.
         *
         * @return the stream id for the ingress channel.
         * @see Configuration#INGRESS_STREAM_ID_PROP_NAME
         */
        public int ingressStreamId()
        {
            return ingressStreamId;
        }

        /**
         * Set the channel parameter for the egress channel.
         *
         * @param channel parameter for the egress channel.
         * @return this for a fluent API.
         * @see Configuration#EGRESS_CHANNEL_PROP_NAME
         */
        public Context egressChannel(final String channel)
        {
            egressChannel = channel;
            return this;
        }

        /**
         * Get the channel parameter for the egress channel.
         *
         * @return the channel parameter for the egress channel.
         * @see Configuration#EGRESS_CHANNEL_PROP_NAME
         */
        public String egressChannel()
        {
            return egressChannel;
        }

        /**
         * Set the stream id for the egress channel.
         *
         * @param streamId for the egress channel.
         * @return this for a fluent API
         * @see Configuration#EGRESS_STREAM_ID_PROP_NAME
         */
        public Context egressStreamId(final int streamId)
        {
            egressStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for the egress channel.
         *
         * @return the stream id for the egress channel.
         * @see Configuration#EGRESS_STREAM_ID_PROP_NAME
         */
        public int egressStreamId()
        {
            return egressStreamId;
        }

        /**
         * Set the {@link IdleStrategy} used when waiting for responses.
         *
         * @param idleStrategy used when waiting for responses.
         * @return this for a fluent API.
         */
        public Context idleStrategy(final IdleStrategy idleStrategy)
        {
            this.idleStrategy = idleStrategy;
            return this;
        }

        /**
         * Get the {@link IdleStrategy} used when waiting for responses.
         *
         * @return the {@link IdleStrategy} used when waiting for responses.
         */
        public IdleStrategy idleStrategy()
        {
            return idleStrategy;
        }

        /**
         * Set the top level Aeron directory used for communication between the Aeron client and Media Driver.
         *
         * @param aeronDirectoryName the top level Aeron directory.
         * @return this for a fluent API.
         */
        public Context aeronDirectoryName(final String aeronDirectoryName)
        {
            this.aeronDirectoryName = aeronDirectoryName;
            return this;
        }

        /**
         * Get the top level Aeron directory used for communication between the Aeron client and Media Driver.
         *
         * @return The top level Aeron directory.
         */
        public String aeronDirectoryName()
        {
            return aeronDirectoryName;
        }

        /**
         * {@link Aeron} client for communicating with the local Media Driver.
         * <p>
         * This client will be closed when the {@link AeronCluster#close()} or {@link #close()} methods are called if
         * {@link #ownsAeronClient()} is true.
         *
         * @param aeron client for communicating with the local Media Driver.
         * @return this for a fluent API.
         * @see Aeron#connect()
         */
        public Context aeron(final Aeron aeron)
        {
            this.aeron = aeron;
            return this;
        }

        /**
         * {@link Aeron} client for communicating with the local Media Driver.
         * <p>
         * If not provided then a default will be established during {@link #conclude()} by calling
         * {@link Aeron#connect()}.
         *
         * @return client for communicating with the local Media Driver.
         */
        public Aeron aeron()
        {
            return aeron;
        }

        /**
         * Does this context own the {@link #aeron()} client and this takes responsibility for closing it?
         *
         * @param ownsAeronClient does this context own the {@link #aeron()} client.
         * @return this for a fluent API.
         */
        public Context ownsAeronClient(final boolean ownsAeronClient)
        {
            this.ownsAeronClient = ownsAeronClient;
            return this;
        }

        /**
         * Does this context own the {@link #aeron()} client and this takes responsibility for closing it?
         *
         * @return does this context own the {@link #aeron()} client and this takes responsibility for closing it?
         */
        public boolean ownsAeronClient()
        {
            return ownsAeronClient;
        }

        /**
         * Is ingress to the cluster exclusively from a single thread for this client?
         *
         * @param isIngressExclusive true if ingress to the cluster is exclusively from a single thread for this client?
         * @return this for a fluent API.
         */
        public Context isIngressExclusive(final boolean isIngressExclusive)
        {
            this.isIngressExclusive = isIngressExclusive;
            return this;
        }

        /**
         * Is ingress to the cluster exclusively from a single thread for this client?
         *
         * @return true if ingress to the cluster exclusively from a single thread for this client?
         */
        public boolean isIngressExclusive()
        {
            return isIngressExclusive;
        }

        /**
         * Get the {@link CredentialsSupplier} to be used for authentication with the cluster.
         *
         * @return the {@link CredentialsSupplier} to be used for authentication with the cluster.
         */
        public CredentialsSupplier credentialsSupplier()
        {
            return credentialsSupplier;
        }

        /**
         * Set the {@link CredentialsSupplier} to be used for authentication with the cluster.
         *
         * @param credentialsSupplier to be used for authentication with the cluster.
         * @return this for fluent API.
         */
        public Context credentialsSupplier(final CredentialsSupplier credentialsSupplier)
        {
            this.credentialsSupplier = credentialsSupplier;
            return this;
        }

        /**
         * Get the {@link ErrorHandler} to be used for handling any exceptions.
         *
         * @return The {@link ErrorHandler} to be used for handling any exceptions.
         */
        public ErrorHandler errorHandler()
        {
            return errorHandler;
        }

        /**
         * Set the {@link ErrorHandler} to be used for handling any exceptions.
         *
         * @param errorHandler Method to handle objects of type Throwable.
         * @return this for fluent API.
         */
        public Context errorHandler(final ErrorHandler errorHandler)
        {
            this.errorHandler = errorHandler;
            return this;
        }

        /**
         * Get the {@link SessionMessageListener} function that will be called when polling for egress via
         * {@link AeronCluster#pollEgress()}.
         *
         * @return the {@link SessionMessageListener} function that will be called when polling for egress via
         * {@link AeronCluster#pollEgress()}.
         */
        public SessionMessageListener sessionMessageListener()
        {
            return sessionMessageListener;
        }

        /**
         * Get the {@link SessionMessageListener} function that will be called when polling for egress via
         * {@link AeronCluster#pollEgress()}.
         *
         * @param listener function that will be called when polling for egress via {@link AeronCluster#pollEgress()}.
         * @return this for a fluent API.
         */
        public Context sessionMessageListener(final SessionMessageListener listener)
        {
            this.sessionMessageListener = listener;
            return this;
        }

        /**
         * Close the context and free applicable resources.
         * <p>
         * If {@link #ownsAeronClient()} is true then the {@link #aeron()} client will be closed.
         */
        public void close()
        {
            if (ownsAeronClient)
            {
                CloseHelper.close(aeron);
            }
        }
    }
}