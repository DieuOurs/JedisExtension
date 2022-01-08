package fr.woria.api.bdd;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.function.Consumer;

public class JedisExtension {

    private static JedisExtension jedisUtils;

    private final JedisPool jedisPool;
    private final String pass;
    private final String clientName;
    private int defaultDB = 0;

    /**
     * @param ip         IP of the Redis instance
     * @param pass       Password of the Redis instance
     * @param clientName Client Name set to connection.
     * @param defaultDB  The default database where jedis connection is connect
     */
    public JedisExtension(String ip, String pass, String clientName, int defaultDB) {
        this.jedisPool = new JedisPool(ip);
        this.pass = pass;
        this.clientName = clientName;
        this.defaultDB = defaultDB;

        Runtime.getRuntime().addShutdownHook(close());
        jedisUtils = this;
    }

    public static JedisExtension getJedisConnection() {
        return jedisUtils;
    }

    /**
     * Execute <code>consumer</code> when an jedis instance is available on default database.
     *
     * @param consumer
     */
    public void execute(Consumer<Jedis> consumer) {
        try (Jedis jedis = jedisPool.getResource()) {
            if (!pass.isEmpty())
                jedis.auth(pass);
            jedis.clientSetname(clientName);
            jedis.select(defaultDB);
            consumer.accept(jedis);
        }
    }

    /**
     * Publish <code>message</code> on <code>channel</code> on the default database.
     *
     * @param channel
     * @param message
     */
    public void publish(String channel, String message) {
        execute(redis -> redis.publish(channel, message));
    }

    /**
     * Close active Jedis connexions.
     */
    private Thread close() {
        return new Thread(() -> {
            jedisPool.close();
            jedisPool.destroy();
        });
    }

    /**
     * Change the default database with <code>defaultDB</code>
     *
     * @param defaultDB
     */
    public JedisExtension setDefaultDB(int defaultDB) {
        this.defaultDB = defaultDB;
        return JedisExtension.getJedisConnection();
    }
}
