package org.clean.flow.entity;

import lombok.Data;
import org.clean.flow.util.CleanFlowUtils;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.util.Enumeration;
import java.util.Properties;

@Data
public class Config {

    private String costCacheFile;
    private String startTime;
    private String period;

    private String postgresDriver;
    private String postgresUrl;
    private String postgresUser;
    private String postgresPassword;
    private String oracleDriver;
    private String oracleUrl;
    private String oracleCreditUser;
    private String oracleCreditPassword;
    private String oracleConSuperUser;
    private String oracleConSuperPassword;

    private String xyDataStore;
    private String xyLogicTopic;
    private String zhDataStore;
    private String zhLogicTopic;
    private String queryCatalogInfo;
    private String commonlyPrimaryKey;
    private String queryPrepareToCleanCatalog;
    private String queryCleanRuleInfo;
    private String needHandler;
    private String errorRecorder;
    private String rightRecorder;
    private String queryTableStruct;
    private String isDateTime;
    private String saveQuality;
    private String saveQualityDispose;
    private String saveQualityResource;

    private static class ConfigHolder {
        private static final Config INSTANCE = new Config();
    }

    public static Config getInstance() {
        return ConfigHolder.INSTANCE;
    }

    private Config() {}

    public Config installConfig() {
        String fileName = System.getProperty("user.dir").concat("/config_Zh_CN.properties");
        Properties properties = CleanFlowUtils.getInstance().loadProperties(fileName);
        Enumeration<String> enumeration = (Enumeration<String>) properties.propertyNames();
        while (enumeration.hasMoreElements()) {
            String key = enumeration.nextElement();
            try {
                Config.class.getDeclaredMethod(firstLetterToUpperCase(key), String.class).invoke(this, new String(properties.getProperty(key).getBytes(), "utf-8"));
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        return this;
    }

    public String firstLetterToUpperCase(String content) {
        return "set".concat(content.substring(0, 1).toUpperCase()).concat(content.substring(1));
    }

}
