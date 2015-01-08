package org.nailedtothex.derby;

import org.jboss.jca.adapters.jdbc.spi.listener.ConnectionListener;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DerbyShutdownConnectionListener implements ConnectionListener {

    private static final Logger log = Logger.getLogger(DerbyShutdownConnectionListener.class.getName());
    private static final String DEFAULT_URL = "jdbc:derby:";
    private static final String SHUTDOWN_SUFFIX = ";shutdown=true";

    private String url;
    private String urlForShutdown;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    synchronized public void initialize(final ClassLoader classLoader) throws SQLException {
        urlForShutdown = createUrlForShutdown();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.log(Level.INFO, "Shutdown derby. URL to use: {0}", urlForShutdown);
                shutdown(urlForShutdown);
            }
        });
        log.log(Level.INFO, "Derby shutdown hook added. URL to use: {0}", urlForShutdown);
    }

    private String createUrlForShutdown() {
        return (url == null ? DEFAULT_URL : url) + SHUTDOWN_SUFFIX;
    }

    private void shutdown(String url) {
        Connection cn = null;
        try {
            cn = DriverManager.getConnection(url);
        } catch (SQLException e) {
            if ("08006".equals(e.getSQLState()) || "XJ015".equals(e.getSQLState())) {
                log.log(Level.INFO, "Derby shutdown succeeded. SQLState={0}", e.getSQLState());
                return;
            }
            log.log(Level.SEVERE, "Derby shutdown failed", e);
        } finally {
            if (cn != null) {
                try {
                    cn.close();
                } catch (Exception e) {
                }
            }
        }
    }

    @Override
    public void activated(Connection connection) throws SQLException {
    }

    @Override
    public void passivated(Connection connection) throws SQLException {
    }
}