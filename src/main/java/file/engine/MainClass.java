package file.engine;

import file.engine.configs.Constants;
import file.engine.dllInterface.WindowCheck;
import file.engine.event.handler.Event;
import file.engine.event.handler.EventManagement;
import file.engine.event.handler.impl.BootSystemEvent;
import file.engine.event.handler.impl.configs.CheckConfigsEvent;
import file.engine.event.handler.impl.configs.SetConfigsEvent;
import file.engine.event.handler.impl.database.CheckDatabaseEmptyEvent;
import file.engine.event.handler.impl.database.InitializeDatabaseEvent;
import file.engine.event.handler.impl.database.UpdateDatabaseEvent;
import file.engine.event.handler.impl.monitor.disk.StartMonitorDiskEvent;
import file.engine.services.DatabaseService;
import file.engine.utils.Md5Util;
import file.engine.utils.clazz.scan.ClassScannerUtil;
import file.engine.utils.file.FileUtil;
import file.engine.utils.system.properties.IsDebug;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.Period;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MainClass {
    private static final int UPDATE_DATABASE_THRESHOLD = 3;

    public static void main(String[] args) {
        try {
            setSystemProperties();

            if (!System.getProperty("os.arch").contains("64")) {
                log.error("Not 64 Bit");
                throw new RuntimeException("Not 64 Bit");
            }

            initFoldersAndFiles();
            releaseAllDependence();
            Class.forName("org.sqlite.JDBC");
            initializeDllInterface();
            initEventManagement();
            //清空tmp
            FileUtil.deleteDir(new File("tmp"));
            setAllConfigs();
            checkConfigs();
            initDatabase();
            monitorDisks();
            // 初始化全部完成，发出启动系统事件
            if (sendBootSystemSignal()) {
                throw new RuntimeException("Boot System Failed");
            }

            mainLoop();
        } catch (Exception e) {
            log.error("error: {}", e.getMessage(), e);
            System.exit(-1);
        }
    }

    /**
     * 加载本地释放的dll
     *
     * @throws ClassNotFoundException 加载失败
     */
    private static void initializeDllInterface() throws ClassNotFoundException {
        Class.forName("file.engine.dllInterface.FileMonitor");
        Class.forName("file.engine.dllInterface.IsLocalDisk");
        Class.forName("file.engine.dllInterface.WindowCheck");
    }

    private static void setSystemProperties() {
        System.setProperty("file.encoding", "UTF-8");
        System.setProperty("org.sqlite.lib.path", new File("").getAbsolutePath());
        System.setProperty("org.sqlite.lib.name", "sqliteJDBC.dll");
    }

    private static void monitorDisks() {
        EventManagement eventManagement = EventManagement.getInstance();
        eventManagement.putEvent(new StartMonitorDiskEvent());
    }

    private static void initDatabase() {
        EventManagement eventManagement = EventManagement.getInstance();
        InitializeDatabaseEvent initializeDatabaseEvent = new InitializeDatabaseEvent();
        eventManagement.putEvent(initializeDatabaseEvent);
        if (eventManagement.waitForEvent(initializeDatabaseEvent, 5 * 60 * 1000)) {
            throw new RuntimeException("Initialize database failed");
        }
    }

    private static void setAllConfigs() {
        EventManagement eventManagement = EventManagement.getInstance();
        SetConfigsEvent setConfigsEvent = new SetConfigsEvent(null);
        eventManagement.putEvent(setConfigsEvent);
        if (eventManagement.waitForEvent(setConfigsEvent)) {
            throw new RuntimeException("Set configs failed");
        }
    }

    private static void initEventManagement() {
        // 初始化事件注册中心，注册所有事件
        EventManagement eventManagement = EventManagement.getInstance();
        eventManagement.readClassList();
        eventManagement.registerAllHandler();
        eventManagement.registerAllListener();
        if (IsDebug.isDebug()) {
            ClassScannerUtil.saveToClassListFile();
        }
        eventManagement.releaseClassesList();
    }

    private static void checkConfigs() {
        EventManagement eventManagement = EventManagement.getInstance();
        eventManagement.putEvent(new CheckConfigsEvent(), event -> {
            Optional<String> optional = event.getReturnValue();
            optional.ifPresent((errorInfo) -> {
                if (!errorInfo.isEmpty()) {
                    log.warn(errorInfo);
                }
            });
        }, null);
    }

    /**
     * 主循环
     * 检查启动时间并更新索引
     */
    private static void mainLoop() throws InterruptedException {
        EventManagement eventManagement = EventManagement.getInstance();

        boolean isNeedUpdate = false;
        boolean isDatabaseOutDated = false;

        if (!IsDebug.isDebug()) {
            isNeedUpdate = isStartOverThreshold();
        }
        CheckDatabaseEmptyEvent checkDatabaseEmptyEvent = new CheckDatabaseEmptyEvent();
        eventManagement.putEvent(checkDatabaseEmptyEvent);
        if (eventManagement.waitForEvent(checkDatabaseEmptyEvent)) {
            throw new RuntimeException("check database empty failed");
        }
        Optional<Object> returnValue = checkDatabaseEmptyEvent.getReturnValue();
        // 不使用lambda表达式，否则需要转换成原子或者进行包装
        if (returnValue.isPresent()) {
            isNeedUpdate |= (boolean) returnValue.get();
        }
        var startTime = LocalDate.now();
        while (eventManagement.notMainExit()) {
            // 主循环开始
            //检查已工作时间
            var endTime = LocalDate.now();
            var diffDays = Period.between(startTime, endTime);
            if (diffDays.getDays() > 2) {
                startTime = endTime;
                //启动时间已经超过2天,更新索引
                isDatabaseOutDated = true;
            }
            // 更新标志isNeedUpdate为true，则更新
            // 数据库损坏或者重启次数超过3次，需要重建索引
            if ((isDatabaseOutDated && !WindowCheck.INSTANCE.isForegroundFullscreen()) || isNeedUpdate) {
                isDatabaseOutDated = false;
                isNeedUpdate = false;
                log.info("Updating index");
                eventManagement.putEvent(new UpdateDatabaseEvent(false),
                        event -> log.info("Index updated"),
                        event -> log.warn("Index update failed"));
            }
            TimeUnit.MILLISECONDS.sleep(50);
        }
    }

    /**
     * 检查启动次数，若已超过三次则发出重新更新索引信号
     *
     * @return true如果启动超过三次
     */
    private static boolean isStartOverThreshold() {
        int startTimes = 0;
        File startTimeCount = new File("user/startTimeCount.dat");
        boolean isFileCreated;
        boolean ret = false;
        if (startTimeCount.exists()) {
            isFileCreated = true;
        } else {
            try {
                isFileCreated = startTimeCount.createNewFile();
            } catch (IOException e) {
                isFileCreated = false;
                log.error("error: {}", e.getMessage(), e);
            }
        }
        if (isFileCreated) {
            try (var reader = new BufferedReader(new InputStreamReader(new FileInputStream(startTimeCount), StandardCharsets.UTF_8))) {
                //读取启动次数
                String times = reader.readLine();
                if (!(times == null || times.isEmpty())) {
                    try {
                        startTimes = Integer.parseInt(times);
                        //使用次数大于3次，优化数据库
                        if (startTimes >= UPDATE_DATABASE_THRESHOLD) {
                            startTimes = 0;
                            if (DatabaseService.getInstance().getStatus() == Constants.Enums.DatabaseStatus.NORMAL) {
                                ret = true;
                            }
                        }
                    } catch (NumberFormatException e) {
                        ret = true;
                    }
                }
                //自增后写入
                startTimes++;
                try (var writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(startTimeCount), StandardCharsets.UTF_8))) {
                    writer.write(String.valueOf(startTimes));
                }
            } catch (Exception e) {
                log.error("error: {}", e.getMessage(), e);
            }
        }
        return ret;
    }

    /**
     * 初始化全部完成，发出启动系统事件
     */
    private static boolean sendBootSystemSignal() {
        EventManagement eventManagement = EventManagement.getInstance();

        Event event = new BootSystemEvent();
        eventManagement.putEvent(event);
        return eventManagement.waitForEvent(event);
    }

    private static void releaseAllDependence() throws IOException {
        checkMd5AndReplace("fileMonitor.dll", "/win32-native/fileMonitor.dll");
        checkMd5AndReplace("isLocalDisk.dll", "/win32-native/isLocalDisk.dll");
        checkMd5AndReplace("fileSearcherUSN.exe", "/win32-native/fileSearcherUSN.exe");
        checkMd5AndReplace("sqlite3.dll", "/win32-native/sqlite3.dll");
        checkMd5AndReplace("windowCheck.dll", "/win32-native/windowCheck.dll");
        checkMd5AndReplace("getWindowsKnownFolder.dll", "/win32-native/getWindowsKnownFolder.dll");
        checkMd5AndReplace("sqliteJDBC.dll", "/win32-native/sqliteJDBC.dll");
        checkMd5AndReplace("cudaAccelerator.dll", "/win32-native/cudaAccelerator.dll");
        checkMd5AndReplace("openclAccelerator.dll", "/win32-native/openclAccelerator.dll");
        checkMd5AndReplace("cudart64_110.dll", "/win32-native/cudart64_110.dll");
        checkMd5AndReplace("cudart64_12.dll", "/win32-native/cudart64_12.dll");
    }

    private static void checkMd5AndReplace(String path, String rootPath) throws IOException {
        try (InputStream insideJar = Objects.requireNonNull(MainClass.class.getResourceAsStream(rootPath))) {
            File target = new File(path);
            String fileMd5 = Md5Util.getMD5(target.getAbsolutePath());
            String md5InsideJar = Md5Util.getMD5(insideJar);
            if (!target.exists() || !md5InsideJar.equals(fileMd5)) {
                if (IsDebug.isDebug()) {
                    log.info("正在重新释放文件：" + path);
                }
                FileUtil.copyFile(MainClass.class.getResourceAsStream(rootPath), target);
            }
        }
    }

    /**
     * 释放所有文件
     */
    private static void initFoldersAndFiles() {
        boolean isSucceeded;
        //user
        isSucceeded = createFileOrFolder("user", false, false);
        //data
        isSucceeded &= createFileOrFolder("data", false, false);
        //tmp
        File tmp = new File("tmp");
        isSucceeded &= createFileOrFolder(tmp, false, false);
        if (!isSucceeded) {
            throw new RuntimeException("初始化依赖项失败");
        }
    }

    private static boolean createFileOrFolder(File file, boolean isFile, boolean isDeleteOnExit) {
        boolean result;
        try {
            if (!file.exists()) {
                if (isFile) {
                    result = file.createNewFile();
                } else {
                    result = file.mkdirs();
                }
                if (isDeleteOnExit) {
                    file.deleteOnExit();
                }
            } else {
                result = true;
            }
        } catch (IOException e) {
            log.error("error: {}", e.getMessage(), e);
            result = false;
        }
        return result;
    }

    private static boolean createFileOrFolder(String path, boolean isFile, @SuppressWarnings("SameParameterValue") boolean isDeleteOnExit) {
        File file = new File(path);
        return createFileOrFolder(file, isFile, isDeleteOnExit);
    }
}
