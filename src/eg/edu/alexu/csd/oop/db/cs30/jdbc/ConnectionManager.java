package eg.edu.alexu.csd.oop.db.cs30.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConnectionManager {

        private static List<java.sql.Connection> connections;
        private static Map<java.sql.Connection, Boolean> connectionBooleanMap;
        private static ConnectionManager connectionManager = null;
        private ConnectionManager()
        {
            connections = new ArrayList<>();
            connectionBooleanMap = new HashMap<>();
        }

        public static ConnectionManager getInstance()
        {
            if (connectionManager == null)
                connectionManager = new ConnectionManager();

            return connectionManager;
        }

        private static void initialize()
        {
            for (int i = 0; i < 5; i++)
            {
                Connection connection = new Connection("");
                connections.add(connection);
                connectionBooleanMap.put(connection, false);
            }
        }

        public java.sql.Connection getConnection(String path)
        {
            for (java.sql.Connection connection : connections) {
                if (!connectionBooleanMap.get(connection)) {
                    connectionBooleanMap.replace(connection, true);
                    ((Connection)connection).setPath(path);
                    return connection;
                }
            }
            Connection connection = new Connection(path);
            connections.add(connection);
            connectionBooleanMap.put(connection, true);
            return connection;
        }

        public void releaseConnection(java.sql.Connection connection) throws SQLException {

            if (connectionBooleanMap.containsKey(connection))
                connectionBooleanMap.replace(connection, false);
            else
                throw new SQLException("ERROR!");

        }
}
