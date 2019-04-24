package org.clean.flow.util;

import com.alibaba.fastjson.JSON;
import org.clean.flow.entity.Config;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.*;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

public final class CleanFlowUtils {

    private static volatile CleanFlowUtils instance;
    private Config config;

    private CleanFlowUtils() {}

    public static CleanFlowUtils getInstance() {
        if (instance == null) {
            synchronized (CleanFlowUtils.class) {
                if (instance == null) {
                    instance = new CleanFlowUtils();
                }
            }
        }
        return instance;
    }

    public void setConfig(Config config) {
        this.config = config;
    }

    public final <T> Vector<T> loadCache(final String filePath, final Class<T> clazz) {
        String cacheJson = getJson(filePath);
        if (cacheJson != null && !cacheJson.equals("")) {
            return toEntityVector(getMapVector(cacheJson), clazz);
        }
        return null;
    }

    public final String getJson(final String filePath) {
        FileInputStream fileInputStream = null;
        try {
            File file = new File(filePath);
            fileInputStream = new FileInputStream(file);
            byte[] bytes = new byte[(int) file.length()];
            fileInputStream.read(bytes);
            return new String(bytes, "utf-8");
        } catch (FileNotFoundException e) {
        } catch (IOException e) {
        } finally {
            closet(fileInputStream);
        }
        return null;
    }

    public final Connection getPostgresConnection() {
        try {
            Class.forName(config.getPostgresDriver());
            return DriverManager.getConnection(config.getPostgresUrl(), config.getPostgresUser(), config.getPostgresPassword());
        } catch (ClassNotFoundException e) {
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public final Connection getXyOracleConnection() {
        try {
            Class.forName(config.getOracleDriver());
            return DriverManager.getConnection(config.getOracleUrl(), config.getOracleCreditUser(), config.getOracleCreditPassword());
        } catch (ClassNotFoundException e) {
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public final Connection getZhOracleConnection() {
        try {
            Class.forName(config.getOracleDriver());
            return DriverManager.getConnection(config.getOracleUrl(), config.getOracleConSuperUser(), config.getOracleConSuperPassword());
        } catch (ClassNotFoundException e) {
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public final void release(final Connection connection) {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public final void release(final Statement statement) {
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public final void release(final ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public final void closet(final Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException e) {
        }
    }

    public final Properties loadProperties(final String fileName) {
        InputStream inputStream = null;
        InputStreamReader inputStreamReader = null;
        Properties properties = null;
        try {
            inputStream = new FileInputStream(fileName);//CleanFlowUtils.class.getClassLoader().getResourceAsStream(fileName);
            inputStreamReader = new InputStreamReader(inputStream, "utf-8");
            properties = new Properties();
            properties.load(inputStreamReader);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closet(inputStreamReader);
            closet(inputStream);
        }
        return properties;
    }

    public final String toJson(final Object value) {
        try {
            return JSON.toJSONString(value);
        } catch (Exception e) {
            //It's impossible to happen
            return null;
        }
    }

    public final Vector<Map> getMapVector(final String content) {
        try {
            return new Vector<Map>(JSON.parseArray(content, Map.class));
        } catch (Exception e) {
            return null;
        }
    }

    public final <T> Vector<T> getVector(final String content, Class<T> clazz) {
        try {
            return new Vector<T>(JSON.parseArray(content, clazz));
        } catch (Exception e) {
            return null;
        }
    }

    public final <T> Vector<T> toEntityVector(final Vector<Map> mapVector, final Class<T> clazz) {
        if (mapVector != null && !mapVector.isEmpty()) {
            Vector<T> tList = new Vector<T>();
            for (Map<String, Object> map : mapVector) {
                tList.add(toEntity(map, clazz));
            }
            return tList;
        }
        return null;
    }

    public final <T> T toEntity(final Map<String, Object> map, final Class<T> clazz) {
        if (map == null || map.isEmpty()) {
            return null;
        }
        try {
            T t = clazz.newInstance();
            BeanInfo beanInfo = Introspector.getBeanInfo(clazz);
            PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
            for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
                String key = propertyDescriptor.getName();
                if (key.equalsIgnoreCase("class")) {
                    continue;
                }
                Method setter = propertyDescriptor.getWriteMethod();
                if (setter != null) {
                    setter.invoke(t, map.get(key));
                }
            }
            return t;
        } catch (Exception e) {
            return null;
        }
    }

}
