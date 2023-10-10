package org.apache.spark.sql.execution.datasources.jdbc.security;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * @author chenqg
 * @date 2018/3/21 16:14
 * @Description: TODO
 */
public class PropertiesUtil {
    public static Properties getPropertiesFromPath(File file) {
        Properties properties = new Properties();
        InputStreamReader in = null;
        try {
            in = new InputStreamReader(new FileInputStream(file));
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return properties;
    }

    public static Properties getPropertiesFromPath(String filePath) {
        return getPropertiesFromPath(new File(filePath));
    }

    public static Properties getPropertiesFromClassPath(String fileName) {
        Properties properties = new Properties();
        InputStreamReader in = null;
        try {
            in = new InputStreamReader(org.apache.spark.sql.execution.datasources.jdbc.security.PropertiesUtil.class.getClassLoader().getResourceAsStream(fileName));
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return properties;
    }

}
