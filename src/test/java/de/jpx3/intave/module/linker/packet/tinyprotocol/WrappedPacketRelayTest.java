package de.jpx3.intave.module.linker.packet.tinyprotocol;

import com.ticxo.modelengine.api.nms.network.ProtectedPacket;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WrappedPacketRelayTest {
  private final List<Object> relayed = new ArrayList<>();
  private final EmbeddedChannel channel = new EmbeddedChannel(new WrappedPacketRelay(relayed::add));

  @Test
  void wrappedPacketIsRelayedAndForwardedUnchanged() {
    Object packet = new Object();
    ProtectedPacket wrapper = new ProtectedPacket(packet);
    channel.writeOutbound(wrapper);
    assertEquals(List.of(packet), relayed);
    assertSame(wrapper, channel.readOutbound());
  }

  @Test
  void bundlesAreSplitIntoTheirSubPackets() {
    Object spawn = new Object();
    Object passengers = new Object();
    channel.writeOutbound(new ProtectedPacket(new ClientboundBundlePacket(List.of(spawn, passengers))));
    assertEquals(List.of(spawn, passengers), relayed);
  }

  @Test
  void unwrappedMessagesAreLeftToTheNormalPipeline() {
    Object packet = new Object();
    channel.writeOutbound(packet);
    assertTrue(relayed.isEmpty());
    assertSame(packet, channel.readOutbound());
  }

  @Test
  void failingSinkStillForwardsTheWrapper() {
    EmbeddedChannel failing = new EmbeddedChannel(new WrappedPacketRelay(packet -> {
      throw new IllegalStateException("listener failure");
    }));
    ProtectedPacket wrapper = new ProtectedPacket(new Object());
    failing.writeOutbound(wrapper);
    assertSame(wrapper, failing.readOutbound());
  }

  @Test
  void injectionSitsOnTheTailSideOfTheUnwrapper() {
    EmbeddedChannel pipeline = new EmbeddedChannel();
    pipeline.pipeline().addLast("protected_packet_unpacker", new io.netty.channel.ChannelOutboundHandlerAdapter());
    pipeline.pipeline().addLast("packet_handler", new io.netty.channel.ChannelOutboundHandlerAdapter());
    WrappedPacketRelay.tryInject(null, pipeline, type -> null);
    pipeline.runPendingTasks();
    List<String> names = pipeline.pipeline().names();
    assertEquals(names.indexOf("protected_packet_unpacker") + 1, names.indexOf(WrappedPacketRelay.HANDLER_NAME));
  }

  private record ClientboundBundlePacket(Iterable<Object> subPackets) {
  }
}
