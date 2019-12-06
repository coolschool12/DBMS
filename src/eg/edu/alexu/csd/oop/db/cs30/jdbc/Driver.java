package eg.edu.alexu.csd.oop.db.cs30.jdbc;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class Driver implements java.sql.Driver {

    private ConnectionManager connectionManager;

    Driver()
    {
        connectionManager = ConnectionManager.getInstance();
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {

        if (!acceptsURL(url))
            throw new SQLException("URL IS NOT SUPPORTED");

        File file = (File) info.get("path");
        String path = file.getAbsolutePath();
        return connectionManager.getConnection(path);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {

        Pattern pattern = Pattern.compile("(^jdbc:xmldb://localhost$)");
        return pattern.matcher(url).matches();
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {

        Set<String> keys = info.stringPropertyNames();
        int i = 0;
        DriverPropertyInfo[] propertyInfo = new DriverPropertyInfo[info.size()];
        for (String key : keys) {
            propertyInfo[i++] = new DriverPropertyInfo(key, info.getProperty(key));
        }

        return propertyInfo;
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

}
