package de.jpx3.intave.module.linker.packet.tinyprotocol;

import com.comphenix.protocol.PacketType;
import de.jpx3.intave.IntavePlugin;
import de.jpx3.intave.check.EventProcessor;
import de.jpx3.intave.executor.Synchronizer;
import de.jpx3.intave.module.linker.bukkit.BukkitEventSubscription;
import de.jpx3.intave.module.linker.packet.FilteringPacketAdapter;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public final class InjectionService implements EventProcessor {
  private final Map<PacketType, Collection<FilteringPacketAdapter>> packetListeners = new ConcurrentHashMap<>();
  private final IntavePlugin plugin;
  private final TinyProtocol pipelineInjector;
  private volatile Function<PacketType, Collection<FilteringPacketAdapter>> relaySubscriptions;

  public InjectionService(IntavePlugin plugin) {
    this.plugin = plugin;
    this.pipelineInjector = new EventTinyProtocol(plugin, this);
//    if (MinecraftVersions.VER1_12_0.atOrAbove()) {
//      return;
//    }
    injectAll();
    this.plugin.eventLinker().registerEventsIn(this);
  }

  Collection<FilteringPacketAdapter> subscriptionsOf(PacketType type) {
    return packetListeners.get(type);
  }

  public void setupSubscriptions(PacketType type, Collection<FilteringPacketAdapter> listeners) {
    packetListeners.put(type, listeners);
  }

  public void setupRelaySubscriptions(Function<PacketType, Collection<FilteringPacketAdapter>> subscriptions) {
    this.relaySubscriptions = subscriptions;
  }

  public void reset() {
    packetListeners.clear();
  }

  @BukkitEventSubscription
  public void onJoin(PlayerJoinEvent join) {
    Player player = join.getPlayer();
    inject(player);
    // Other plugins add their pipeline handlers on join too, so look for them a few times
    for (int delay : new int[]{1, 20, 100}) {
      Synchronizer.synchronizeDelayed(() -> injectRelay(player), delay);
    }
  }

  private void injectRelay(Player player) {
    Function<PacketType, Collection<FilteringPacketAdapter>> subscriptions = relaySubscriptions;
    if (subscriptions == null || !player.isOnline()) {
      return;
    }
    WrappedPacketRelay.tryInject(player, pipelineInjector.getChannel(player), subscriptions);
  }

  @BukkitEventSubscription
  public void onQuit(PlayerQuitEvent quit) {
    uninject(quit.getPlayer());
  }

  public void injectAll() {
    Bukkit.getOnlinePlayers().forEach(this::inject);
    Bukkit.getOnlinePlayers().forEach(this::injectRelay);
  }

  public void inject(Player player) {
    if (!pipelineInjector.hasInjected(player)) {
      pipelineInjector.injectPlayer(player);
    }
  }

  public void uninjectAll() {
    Bukkit.getOnlinePlayers().forEach(this::uninject);
  }

  public void uninject(Player player) {
//    if (pipelineInjector.hasInjected(player)) {
//      pipelineInjector.uninjectPlayer(player);
//    }
  }
}
