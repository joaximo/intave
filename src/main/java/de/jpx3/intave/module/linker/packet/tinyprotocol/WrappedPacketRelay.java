package de.jpx3.intave.module.linker.packet.tinyprotocol;

import com.comphenix.protocol.PacketType;
import com.comphenix.protocol.events.PacketContainer;
import com.comphenix.protocol.events.PacketEvent;
import com.comphenix.protocol.injector.packet.PacketRegistry;
import de.jpx3.intave.IntaveLogger;
import de.jpx3.intave.module.linker.packet.FilteringPacketAdapter;
import de.jpx3.intave.module.tracker.entity.Entity;
import de.jpx3.intave.user.UserRepository;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.bukkit.entity.Player;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Some plugins (ModelEngine) wrap packets in their own message object and unwrap them in a handler
 * right before the connection, so neither ProtocolLib nor Intave ever see the real packet.
 * This handler sits on top of such an unwrapper and forwards the inner packets to Intave's
 * ProtocolLib subscriptions, so entity spawns, mounts and moves of these packet entities are tracked.
 */
final class WrappedPacketRelay extends ChannelOutboundHandlerAdapter {
  static final String HANDLER_NAME = "intave_wrapped_packet_relay";
  // handler name -> wrapper class name
  private static final Map<String, String> KNOWN_UNWRAPPERS = Map.of(
    "protected_packet_unpacker", "com.ticxo.modelengine.api.nms.network.ProtectedPacket"
  );

  private final Consumer<Object> sink;
  private volatile Method unwrapMethod;
  private volatile Method subPacketsMethod;

  WrappedPacketRelay(Consumer<Object> sink) {
    this.sink = sink;
  }

  static void tryInject(Player player, Channel channel, Function<PacketType, Collection<FilteringPacketAdapter>> subscriptions) {
    channel.eventLoop().execute(() -> {
      if (!channel.isOpen() || channel.pipeline().get(HANDLER_NAME) != null) {
        return;
      }
      for (String unwrapper : KNOWN_UNWRAPPERS.keySet()) {
        if (channel.pipeline().get(unwrapper) != null) {
          // addAfter = closer to the tail, outbound messages reach us before the unwrapper
          WrappedPacketRelay relay = new WrappedPacketRelay(packet -> dispatch(player, subscriptions, packet));
          channel.pipeline().addAfter(unwrapper, HANDLER_NAME, relay);
          return;
        }
      }
    });
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    try {
      if (msg != null && KNOWN_UNWRAPPERS.containsValue(msg.getClass().getName())) {
        relay(unwrap(msg));
      }
    } catch (Throwable throwable) {
      IntaveLogger logger = IntaveLogger.logger();
      if (logger != null) {
        logger.warn("Failed to relay wrapped packet " + msg.getClass().getName() + ": " + throwable);
      }
    }
    super.write(ctx, msg, promise);
  }

  private Object unwrap(Object wrapper) throws Exception {
    Method method = unwrapMethod;
    if (method == null) {
      method = wrapper.getClass().getMethod("packet");
      method.setAccessible(true);
      unwrapMethod = method;
    }
    return method.invoke(wrapper);
  }

  private void relay(Object packet) throws Exception {
    if (packet == null) {
      return;
    }
    if (packet.getClass().getSimpleName().equals("ClientboundBundlePacket")) {
      Method method = subPacketsMethod;
      if (method == null) {
        method = packet.getClass().getMethod("subPackets");
        method.setAccessible(true);
        subPacketsMethod = method;
      }
      for (Object subPacket : (Iterable<?>) method.invoke(packet)) {
        relay(subPacket);
      }
      return;
    }
    sink.accept(packet);
  }

  private static void dispatch(
    Player player,
    Function<PacketType, Collection<FilteringPacketAdapter>> subscriptions,
    Object packet
  ) {
    PacketType packetType = PacketRegistry.getPacketType(packet.getClass());
    if (packetType == null) {
      return;
    }
    Collection<FilteringPacketAdapter> targets = subscriptions.apply(packetType);
    if (targets == null || targets.isEmpty()) {
      return;
    }
    // Cancellation and packet replacement are not supported here: the wrapper is sent as is.
    PacketContainer container = PacketContainer.fromPacket(packet);
    PacketEvent event = PacketEvent.fromServer(WrappedPacketRelay.class, container, player);
    for (FilteringPacketAdapter target : targets) {
      target.onPacketSending(event);
    }
    if (packetType == PacketType.Play.Server.SPAWN_ENTITY) {
      Integer entityId = container.getIntegers().readSafely(0);
      Entity entity = entityId == null ? null : UserRepository.userOf(player).meta().connection().entityBy(entityId);
      if (entity != null) {
        entity.packetOnly = true;
      }
    }
  }
}
