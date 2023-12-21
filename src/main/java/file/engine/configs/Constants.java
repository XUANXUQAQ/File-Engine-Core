package file.engine.configs;

import file.engine.utils.system.properties.IsDebug;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 项目使用到的常量
 */
@Slf4j
public class Constants {
    public static String version;

    public static String buildVersion;

    // 数据库中最大的分表ID  list[0-40]
    public static final int MAX_TABLE_NUM = 40;

    // 关闭数据库连接超时时间
    public static final int CLOSE_DATABASE_TIMEOUT_MILLS = 60 * 1000;

    public static final int MAX_SEARCH_TEXT_LENGTH = 300;

    public static final String DATABASE_CREATE_TIME_FILE = "user/databaseCreateTime.dat";

    public static final String DATABASE_INTEGRITY_CHECK_FILE = "user/databaseIntegrityCheck.dat";

    public static final int MAX_TASK_EXIST_TIME = 5 * 60 * 1000;

    static {
        version = "0";
        buildVersion = "Debug";
        if (!IsDebug.isDebug()) {
            /*
             * 读取maven自动生成的版本信息
             */
            Properties properties = new Properties();
            try (InputStream projectInfo = Constants.class.getResourceAsStream("/project-info.properties")) {
                properties.load(projectInfo);
                version = properties.getProperty("project.version");
                buildVersion = properties.getProperty("project.build.version");
            } catch (IOException e) {
                log.error("error: {}", e.getMessage(), e);
            }
        }
    }

    private Constants() {
        throw new RuntimeException("not allowed");
    }

    public static class Enums {

        /**
         * 数据库运行状态
         * NORMAL：正常
         * _TEMP：正在搜索中，已经切换到临时数据库
         * VACUUM：正在整理数据库
         * MANUAL_UPDATE：正在搜索中，未切换到临时数据库
         */
        public enum DatabaseStatus {
            NORMAL, _TEMP, VACUUM, MANUAL_UPDATE
        }

    }
}
