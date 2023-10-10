package org.apache.spark.sql.execution.datasources.jdbc.security;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.MessageFormat;
import java.util.Properties;

import static org.apache.spark.sql.execution.datasources.jdbc.security.LoginUtil.JAVA_SECURITY_KRB5_CONF_KEY;
import static org.apache.spark.sql.execution.datasources.jdbc.security.LoginUtil.ZOOKEEPER_SERVER_PRINCIPAL_KEY;


/**
 * @Author: HuangHy
 * @CreateTime: 2023-02-15  16:17
 * @Description: 腾讯Solr认证工具
 */
public class TxHiveAuthUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(org.apache.spark.sql.execution.datasources.jdbc.security.TxHiveAuthUtil.class);


    /**
     * 统一从命令行中获取以下参数jaasFile、krb5File、userKeyTab、principal
     * <p>
     * 认证失败请排查一下原因：
     * 1.jar包不是从当前的华为集群下载下来的，小版本不对也不行
     * 2.当前服务器时间与华为集群的服务器时间差距超过5分钟，建议先做服务器的ntp同步。
     * 3.jaas.conf里的principal和user.keytab是否有填错。
     */
    public static UserGroupInformation login(String txConfigPath) throws Exception {
        Thread.currentThread().setContextClassLoader(org.apache.spark.sql.execution.datasources.jdbc.security.TxHiveAuthUtil.class.getClassLoader());
        //每次腾讯验证前，把华为验证设置的信息全部去掉
        //这个方法会导致并发情况下获取不到realm，暂时也用不上，不用也不改了。
//        LOGGER.info("清洗其他hive信息");
//        clearHwConf();
//        LOGGER.info("清洗其他hive信息完毕");
        String etlPath = txConfigPath + File.separator + "tx/etl.properties";
        File etlFile = new File(etlPath);
        if (!etlFile.exists()) {
            LOGGER.error("未配置腾讯Hive鉴权目录以及对应etl.properties,目前路径为:" + etlPath);
            return null;
        }
        Properties propertiesFromPath = PropertiesUtil.getPropertiesFromPath(etlFile);
        String principal = propertiesFromPath.getProperty("principal");
        String keytabUrl = txConfigPath + File.separator + "tx" + File.separator + propertiesFromPath.getProperty("keytab");
        String krb5Url = txConfigPath + File.separator + "tx" + File.separator + propertiesFromPath.getProperty("krb5");
        String authName = propertiesFromPath.getProperty("authName");
        authName = StringUtils.isEmpty(authName) ? "kerberos" : authName;
        LOGGER.info(String.format("txConfigPath:%s，准备重新认证...", txConfigPath));
        Configuration conf = loadConfiguration(txConfigPath + File.separator + "tx" + File.separator);
        LOGGER.info("执行完毕authentication" + authName);
        conf.set("hadoop.security.authentication", authName);
        LoginUtil.setJaasConf("Client_new",principal,keytabUrl);
        return LoginUtil.login(principal, keytabUrl, krb5Url, conf);
       // return conf;
    }

//    private static void clearHwConf() {
//        UserGroupInformation.reset();
//        //hw
//        System.clearProperty(JAVA_SECURITY_KRB5_CONF_KEY);
//        System.clearProperty(ZOOKEEPER_SERVER_PRINCIPAL_KEY);
//        System.clearProperty("java.security.auth.login.config");
//        //浪潮
//        System.clearProperty("java.security.krb5.conf");
//        System.clearProperty("keytab.file");
//
//    }

    public static Configuration loadConfiguration(String classPath) {
        String coreSite = classPath + "core-site.xml";
        //没有放在classPath下
        if (!new File(coreSite).exists()) {
            coreSite = classPath + "core-site.xml";
        }
        LOGGER.info("use config path {}", classPath);
        String hdfsSite = classPath + "hdfs-site.xml";
        //非必要
        String hiveSite = classPath + "hive-site.xml";
        Configuration conf = new Configuration();

        if (new File(coreSite).exists()) {
            conf.addResource(new Path(coreSite));
        } else {
            throw new RuntimeException(MessageFormat.format("core-site.xml not exist in {0}", classPath));
        }


        if (new File(hdfsSite).exists()) {
            conf.addResource(new Path(hdfsSite));
        } else {
            throw new RuntimeException(MessageFormat.format("hdfs-site.xml not exist in {0}", classPath));
        }

        if (new File(hiveSite).exists()) {
            conf.addResource(new Path(hiveSite));
        }

        return conf;

    }


}
