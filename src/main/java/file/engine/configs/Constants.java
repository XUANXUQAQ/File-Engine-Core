package file.engine.configs;

import lombok.extern.slf4j.Slf4j;

/**
 * 项目使用到的常量
 */
@Slf4j
public class Constants {
    // 数据库中最大的分表ID  list[0-40]
    public static final int MAX_TABLE_NUM = 40;

    // 关闭数据库连接超时时间
    public static final int CLOSE_DATABASE_TIMEOUT_MILLS = 60 * 1000;

    public static final int MAX_SEARCH_TEXT_LENGTH = 300;

    public static final String DATABASE_CREATE_TIME_FILE = "user/databaseCreateTime.dat";

    public static final String DATABASE_INTEGRITY_CHECK_FILE = "user/databaseIntegrityCheck.dat";

    public static final String CONFIG_FILE = "user/settings.json";

    public static final String PORT_FILE_NAME = "tmp/$$port";

    public static final int MAX_TASK_EXIST_TIME = 5 * 60 * 1000;

    public static final int THREAD_POOL_AWAIT_TIMEOUT = 5;

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
