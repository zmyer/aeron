/*
 * Copyright 2016 Real Logic Ltd.
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


#include <gtest/gtest.h>
#include <fakeit.hpp>

#include <protocol/SetupFlyweight.h>
#include <protocol/DataHeaderFlyweight.h>

#include <concurrent/logbuffer/LogBufferDescriptor.h>
#include <concurrent/AtomicBuffer.h>

#include <DataPacketDispatcher.h>
#include <PublicationImage.h>
#include <Receiver.h>
#include <DriverConductorProxy.h>

#include <media/ReceiveChannelEndpoint.h>
#include <media/InetAddress.h>

using namespace aeron::protocol;
using namespace aeron::concurrent;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::driver;
using namespace aeron::driver::media;
using namespace fakeit;
using namespace testing;

#define CAPACITY (100)
#define TOTAL_BUFFER_LENGTH (CAPACITY + DataHeaderFlyweight::headerLength())
#define SESSION_ID (1)
#define STREAM_ID (10)
#define ACTIVE_TERM_ID (3)
#define INITIAL_TERM_ID (3)
#define TERM_OFFSET (0)
#define MTU_LENGTH (1024)
#define TERM_LENGTH (LogBufferDescriptor::TERM_MIN_LENGTH)

typedef std::array<std::uint8_t, TOTAL_BUFFER_LENGTH> buffer_t;

class DataPacketDispatcherTest : public Test
{
public:
    DataPacketDispatcherTest() :
        m_dataBufferAtomic(&m_dataBuffer[0], TOTAL_BUFFER_LENGTH),
        m_setupBufferAtomic(&m_setupBuffer[0], TOTAL_BUFFER_LENGTH),
        m_dataHeaderFlyweight(m_dataBufferAtomic, 0),
        m_setupFlyweight(m_setupBufferAtomic, 0),
        m_dataPacketDispatcher(
            std::shared_ptr<DriverConductorProxy>(&m_driverConductorProxy.get()),
            std::shared_ptr<Receiver>(&m_receiver.get()))
    {
        m_dataBuffer.fill(0);
        m_setupBuffer.fill(0);
    }

    virtual ~DataPacketDispatcherTest() throw() {}

    virtual void SetUp()
    {
        m_dataBuffer.fill(0);
        m_setupBuffer.fill(0);

        m_dataHeaderFlyweight
            .streamId(STREAM_ID)
            .sessionId(SESSION_ID)
            .termId(ACTIVE_TERM_ID)
            .termOffset(TERM_OFFSET);

        m_setupFlyweight
            .streamId(STREAM_ID)
            .sessionId(SESSION_ID)
            .actionTermId(ACTIVE_TERM_ID)
            .initialTermId(INITIAL_TERM_ID)
            .termOffset(TERM_OFFSET)
            .mtu(MTU_LENGTH)
            .termLength(TERM_LENGTH);

        When(Method(m_publicationImage, sessionId)).AlwaysReturn(SESSION_ID);
        When(Method(m_publicationImage, streamId)).AlwaysReturn(STREAM_ID);
        When(Method(m_receiveChannelEndpoint, isMulticast)).AlwaysReturn(false);
        Fake(Dtor(m_receiveChannelEndpoint));
        Fake(Dtor(m_driverConductorProxy));
        Fake(Dtor(m_receiver));
        Fake(Dtor(m_publicationImage));
    }

protected:
    AERON_DECL_ALIGNED(buffer_t m_dataBuffer, 16);
    AERON_DECL_ALIGNED(buffer_t m_setupBuffer, 16);
    AtomicBuffer m_dataBufferAtomic;
    AtomicBuffer m_setupBufferAtomic;
    DataHeaderFlyweight m_dataHeaderFlyweight;
    SetupFlyweight m_setupFlyweight;
    Mock<Receiver> m_receiver;
    Mock<DriverConductorProxy> m_driverConductorProxy;
    Mock<ReceiveChannelEndpoint> m_receiveChannelEndpoint;
    Mock<PublicationImage> m_publicationImage;
    DataPacketDispatcher m_dataPacketDispatcher;
};

TEST_F(DataPacketDispatcherTest, shouldElicitSetupMessageWhenDataArrivesForSubscriptionWithoutImage)
{
    std::unique_ptr<InetAddress> src = InetAddress::parse("127.0.0.1");

    When(Method(m_receiveChannelEndpoint, sendSetupElicitingStatusMessage)).AlwaysReturn();
    When(Method(m_receiver, addPendingSetupMessage)).AlwaysReturn();

    m_dataPacketDispatcher.addSubscription(STREAM_ID);
    m_dataPacketDispatcher.onDataPacket(
        m_receiveChannelEndpoint.get(), m_dataHeaderFlyweight, m_dataBufferAtomic, CAPACITY, *src);

    Verify(Method(m_publicationImage, insertPacket)).Exactly(0);
    Verify(Method(m_receiveChannelEndpoint, sendSetupElicitingStatusMessage).Using(_, SESSION_ID, STREAM_ID)).Once();
    Verify(Method(m_receiver, addPendingSetupMessage).Using(SESSION_ID, STREAM_ID, _)).Once();
}

TEST_F(DataPacketDispatcherTest, shouldOnlyElicitSetupMessageOnceWhenDataArrivesForSubscriptionWithoutImage)
{
    std::unique_ptr<InetAddress> src = InetAddress::parse("127.0.0.1");

    When(Method(m_receiveChannelEndpoint, sendSetupElicitingStatusMessage)).AlwaysReturn();
    When(Method(m_receiver, addPendingSetupMessage)).AlwaysReturn();

    m_dataPacketDispatcher.addSubscription(STREAM_ID);
    m_dataPacketDispatcher.onDataPacket(
        m_receiveChannelEndpoint.get(), m_dataHeaderFlyweight, m_dataBufferAtomic, CAPACITY, *src);
    m_dataPacketDispatcher.onDataPacket(
        m_receiveChannelEndpoint.get(), m_dataHeaderFlyweight, m_dataBufferAtomic, CAPACITY, *src);
    m_dataPacketDispatcher.onDataPacket(
        m_receiveChannelEndpoint.get(), m_dataHeaderFlyweight, m_dataBufferAtomic, CAPACITY, *src);

    Verify(Method(m_publicationImage, insertPacket)).Exactly(0);
    Verify(Method(m_receiveChannelEndpoint, sendSetupElicitingStatusMessage).Using(_, SESSION_ID, STREAM_ID)).Once();
    Verify(Method(m_receiver, addPendingSetupMessage).Using(SESSION_ID, STREAM_ID, _)).Once();
}

TEST_F(DataPacketDispatcherTest, shouldElicitSetupMessageAgainWhenDataArrivesForSubscriptionWithoutImageAfterRemovePendingSetup)
{
    std::unique_ptr<InetAddress> src = InetAddress::parse("127.0.0.1");

    When(Method(m_receiveChannelEndpoint, sendSetupElicitingStatusMessage)).AlwaysReturn();
    When(Method(m_receiver, addPendingSetupMessage)).AlwaysReturn();

    m_dataPacketDispatcher.addSubscription(STREAM_ID);
    m_dataPacketDispatcher.onDataPacket(
        m_receiveChannelEndpoint.get(), m_dataHeaderFlyweight, m_dataBufferAtomic, CAPACITY, *src);
    m_dataPacketDispatcher.onDataPacket(
        m_receiveChannelEndpoint.get(), m_dataHeaderFlyweight, m_dataBufferAtomic, CAPACITY, *src);
    m_dataPacketDispatcher.removePendingSetup(SESSION_ID, STREAM_ID);
    m_dataPacketDispatcher.onDataPacket(
        m_receiveChannelEndpoint.get(), m_dataHeaderFlyweight, m_dataBufferAtomic, CAPACITY, *src);
    m_dataPacketDispatcher.onDataPacket(
        m_receiveChannelEndpoint.get(), m_dataHeaderFlyweight, m_dataBufferAtomic, CAPACITY, *src);

    Verify(Method(m_publicationImage, insertPacket)).Exactly(0);
    Verify(Method(m_receiveChannelEndpoint, sendSetupElicitingStatusMessage).Using(_, SESSION_ID, STREAM_ID)).Exactly(2);
    Verify(Method(m_receiver, addPendingSetupMessage).Using(SESSION_ID, STREAM_ID, _)).Exactly(2);
}

TEST_F(DataPacketDispatcherTest, shouldRequestCreateImageUponReceivingSetup)
{
    std::unique_ptr<InetAddress> src = InetAddress::parse("127.0.0.1");

    When(Method(m_driverConductorProxy, createPublicationImage)).AlwaysReturn();

    m_dataPacketDispatcher.addSubscription(STREAM_ID);
    m_dataPacketDispatcher.onSetupMessage(m_receiveChannelEndpoint.get(), m_setupFlyweight, m_setupBufferAtomic, *src);

    Verify(Method(m_driverConductorProxy, createPublicationImage)
             .Using(
                 SESSION_ID, STREAM_ID, INITIAL_TERM_ID, ACTIVE_TERM_ID, TERM_OFFSET, TERM_LENGTH,
                    MTU_LENGTH, _, _, _)).Once();
}


TEST_F(DataPacketDispatcherTest, shouldOnlyRequestCreateImageOnceUponReceivingSetup)
{
    std::unique_ptr<InetAddress> src = InetAddress::parse("127.0.0.1");

    When(Method(m_driverConductorProxy, createPublicationImage)).AlwaysReturn();

    m_dataPacketDispatcher.addSubscription(STREAM_ID);
    m_dataPacketDispatcher.onSetupMessage(m_receiveChannelEndpoint.get(), m_setupFlyweight, m_setupBufferAtomic, *src);
    m_dataPacketDispatcher.onSetupMessage(m_receiveChannelEndpoint.get(), m_setupFlyweight, m_setupBufferAtomic, *src);
    m_dataPacketDispatcher.onSetupMessage(m_receiveChannelEndpoint.get(), m_setupFlyweight, m_setupBufferAtomic, *src);

    Verify(Method(m_driverConductorProxy, createPublicationImage)
             .Using(
                 SESSION_ID, STREAM_ID, INITIAL_TERM_ID, ACTIVE_TERM_ID, TERM_OFFSET, TERM_LENGTH,
                    MTU_LENGTH, _, _, _)).Once();
}

TEST_F(DataPacketDispatcherTest, shouldNotRequestCreateImageOnceUponReceivingSetupAfterImageAdded)
{
    std::unique_ptr<InetAddress> src = InetAddress::parse("127.0.0.1");

    When(Method(m_driverConductorProxy, createPublicationImage)).AlwaysReturn();
    When(Method(m_publicationImage, status)).AlwaysReturn();

    m_dataPacketDispatcher.addSubscription(STREAM_ID);
    m_dataPacketDispatcher.addPublicationImage(std::shared_ptr<PublicationImage>{&m_publicationImage.get()});
    m_dataPacketDispatcher.onSetupMessage(m_receiveChannelEndpoint.get(), m_setupFlyweight, m_setupBufferAtomic, *src);

    Verify(Method(m_driverConductorProxy, createPublicationImage)
               .Using(_, _, _, _, _, _, _, _, _, _)).Exactly(0);
}

TEST_F(DataPacketDispatcherTest, shouldSetImageInactiveOnRemoveSubscription)
{
    When(Method(m_publicationImage, status)).AlwaysReturn();
    When(Method(m_publicationImage, ifActiveGoInactive)).AlwaysReturn();

    m_dataPacketDispatcher.addSubscription(STREAM_ID);
    m_dataPacketDispatcher.addPublicationImage(std::shared_ptr<PublicationImage>{&m_publicationImage.get()});
    m_dataPacketDispatcher.removeSubscription(STREAM_ID);

    Verify(Method(m_publicationImage, ifActiveGoInactive)).Once();
}

TEST_F(DataPacketDispatcherTest, shouldSetImageInactiveOnRemoveImage)
{
    When(Method(m_publicationImage, status)).AlwaysReturn();
    When(Method(m_publicationImage, ifActiveGoInactive)).AlwaysReturn();

    m_dataPacketDispatcher.addSubscription(STREAM_ID);
    m_dataPacketDispatcher.addPublicationImage(std::shared_ptr<PublicationImage>{&m_publicationImage.get()});
    m_dataPacketDispatcher.removePublicationImage(std::shared_ptr<PublicationImage>{&m_publicationImage.get()});

    Verify(Method(m_publicationImage, ifActiveGoInactive)).Once();
}

TEST_F(DataPacketDispatcherTest, shouldIgnoreDataAndSetupAfterImageRemoved)
{
    std::unique_ptr<InetAddress> src = InetAddress::parse("127.0.0.1");

    When(Method(m_publicationImage, status)).AlwaysReturn();
    When(Method(m_publicationImage, ifActiveGoInactive)).AlwaysReturn();

    m_dataPacketDispatcher.addSubscription(STREAM_ID);
    m_dataPacketDispatcher.addPublicationImage(std::shared_ptr<PublicationImage>{&m_publicationImage.get()});
    m_dataPacketDispatcher.removePublicationImage(std::shared_ptr<PublicationImage>{&m_publicationImage.get()});
    m_dataPacketDispatcher.onDataPacket(
        m_receiveChannelEndpoint.get(), m_dataHeaderFlyweight, m_dataBufferAtomic, CAPACITY, *src);
    m_dataPacketDispatcher.onSetupMessage(m_receiveChannelEndpoint.get(), m_setupFlyweight, m_setupBufferAtomic, *src);

    Verify(Method(m_receiver, addPendingSetupMessage)).Exactly(0);
    Verify(Method(m_driverConductorProxy, createPublicationImage)).Exactly(0);
}

TEST_F(DataPacketDispatcherTest, shouldNotIgnoreDataAndSetupAfterImageRemovedAndCooldownRemoved)
{
    std::unique_ptr<InetAddress> src = InetAddress::parse("127.0.0.1");

    When(Method(m_receiveChannelEndpoint, sendSetupElicitingStatusMessage)).AlwaysReturn();
    When(Method(m_receiver, addPendingSetupMessage)).AlwaysReturn();
    When(Method(m_driverConductorProxy, createPublicationImage)).AlwaysReturn();
    When(Method(m_publicationImage, status)).AlwaysReturn();
    When(Method(m_publicationImage, ifActiveGoInactive)).AlwaysReturn();

    m_dataPacketDispatcher.addSubscription(STREAM_ID);
    m_dataPacketDispatcher.addPublicationImage(std::shared_ptr<PublicationImage>{&m_publicationImage.get()});
    m_dataPacketDispatcher.removePublicationImage(std::shared_ptr<PublicationImage>{&m_publicationImage.get()});
    m_dataPacketDispatcher.removeCoolDown(SESSION_ID, STREAM_ID);
    m_dataPacketDispatcher.onDataPacket(
        m_receiveChannelEndpoint.get(), m_dataHeaderFlyweight, m_dataBufferAtomic, CAPACITY, *src);
    m_dataPacketDispatcher.onSetupMessage(m_receiveChannelEndpoint.get(), m_setupFlyweight, m_setupBufferAtomic, *src);

    Verify(Method(m_publicationImage, insertPacket)).Exactly(0);

    Verify(
        Method(m_receiveChannelEndpoint, sendSetupElicitingStatusMessage).Using(_, SESSION_ID, STREAM_ID),
        Method(m_receiver, addPendingSetupMessage).Using(SESSION_ID, STREAM_ID, _),
        Method(m_driverConductorProxy, createPublicationImage)
            .Using(
                SESSION_ID, STREAM_ID, INITIAL_TERM_ID, ACTIVE_TERM_ID, TERM_OFFSET, TERM_LENGTH,
                MTU_LENGTH, _, _, _)
    ).Once();
}


TEST_F(DataPacketDispatcherTest, shouldDispatchDataToCorrectImage)
{
    std::unique_ptr<InetAddress> src = InetAddress::parse("127.0.0.1");

    When(Method(m_publicationImage, status)).AlwaysReturn();
    When(Method(m_publicationImage, insertPacket)).AlwaysReturn(100);

    m_dataPacketDispatcher.addSubscription(STREAM_ID);
    m_dataPacketDispatcher.addPublicationImage(std::shared_ptr<PublicationImage>{&m_publicationImage.get()});
    m_dataPacketDispatcher.onDataPacket(
        m_receiveChannelEndpoint.get(), m_dataHeaderFlyweight, m_dataBufferAtomic, CAPACITY, *src);

    Verify(Method(m_publicationImage, status).Using(PublicationImageStatus::ACTIVE)).Once();
    Verify(Method(m_publicationImage, insertPacket).Using(ACTIVE_TERM_ID, TERM_OFFSET, _, CAPACITY)).Once();
}

