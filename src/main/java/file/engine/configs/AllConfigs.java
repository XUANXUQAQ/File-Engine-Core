package file.engine.configs;

import file.engine.annotation.EventListener;
import file.engine.annotation.EventRegister;
import file.engine.dllInterface.GetWindowsKnownFolder;
import file.engine.dllInterface.IsLocalDisk;
import file.engine.dllInterface.gpu.GPUAccelerator;
import file.engine.event.handler.Event;
import file.engine.event.handler.EventManagement;
import file.engine.event.handler.impl.BootSystemEvent;
import file.engine.event.handler.impl.configs.CheckConfigsEvent;
import file.engine.event.handler.impl.configs.SetConfigsEvent;
import file.engine.event.handler.impl.stop.CloseEvent;
import file.engine.utils.RegexUtil;
import file.engine.utils.file.FileUtil;
import file.engine.utils.gson.GsonUtil;
import file.engine.utils.system.properties.IsDebug;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 保存软件运行时的所有配置信息
 */
@Getter
@Slf4j
public class AllConfigs {
    private volatile ConfigEntity configEntity;
    private static volatile AllConfigs instance = null;

    private AllConfigs() {
    }

    public static AllConfigs getInstance() {
        if (instance == null) {
            synchronized (AllConfigs.class) {
                if (instance == null) {
                    instance = new AllConfigs();
                }
            }
        }
        return instance;
    }

    /**
     * 获取在配置文件中但是实际不存在的磁盘（如移动硬盘）
     *
     * @return set
     */
    public Set<String> getUnAvailableDiskSet() {
        String disks = configEntity.getDisks();
        String[] splitDisks = RegexUtil.comma.split(disks);
        Set<String> set = ConcurrentHashMap.newKeySet();
        for (String root : splitDisks) {
            if (!isDiskAvailable(root)) {
                set.add(root);
            }
        }
        return set;
    }

    /**
     * 获取可搜索磁盘
     *
     * @return 每个磁盘使用逗号隔开，如[C:\,D:\]
     */
    public String getAvailableDisks() {
        String disks = configEntity.getDisks();
        String[] splitDisks = RegexUtil.comma.split(disks);
        StringBuilder stringBuilder = new StringBuilder();
        for (String root : splitDisks) {
            if (isDiskAvailable(root)) {
                stringBuilder.append(root).append(",");
            }
        }
        return stringBuilder.toString();
    }

    /**
     * 判断磁盘是否存在且为NTFS文件系统
     *
     * @param root 磁盘路径，C:\  D:\
     * @return true如果存在且是NTFS磁盘
     */
    private boolean isDiskAvailable(String root) {
        return FileUtil.isFileExist(root) && IsLocalDisk.INSTANCE.isDiskNTFS(root);
    }

    /**
     * 获取所有可用的本地磁盘
     *
     * @return String，用逗号隔开
     */
    private String getLocalDisks() {
        File[] files = File.listRoots();
        if (files == null || files.length == 0) {
            return "";
        }
        String diskName;
        StringBuilder stringBuilder = new StringBuilder();
        for (File each : files) {
            diskName = each.getAbsolutePath();
            if (IsLocalDisk.INSTANCE.isDiskNTFS(diskName) && IsLocalDisk.INSTANCE.isLocalDisk(diskName)) {
                stringBuilder.append(each.getAbsolutePath()).append(",");
            }
        }
        return stringBuilder.toString();
    }

    /**
     * 获取用户配置的磁盘信息
     *
     * @param settingsInJson 用户配置json
     */
    private void readDisks(Map<String, Object> settingsInJson) {
        String disks = getFromJson(settingsInJson, "disks", getLocalDisks());
        String[] stringDisk = RegexUtil.comma.split(disks);
        StringBuilder stringBuilder = new StringBuilder();
        for (String each : stringDisk) {
            stringBuilder.append(each).append(",");
        }
        configEntity.setDisks(stringBuilder.toString());
    }

    private void readIsEnableGpuAccelerate(Map<String, Object> settingsInJson) {
        boolean isEnableGpuAccelerate = getFromJson(settingsInJson, "isEnableGpuAccelerate", true);
        if (isEnableGpuAccelerate) {
            configEntity.setEnableGpuAccelerate(GPUAccelerator.INSTANCE.isGPUAvailableOnSystem());
        } else {
            configEntity.setEnableGpuAccelerate(false);
        }
    }

    private void readGpuDevice(Map<String, Object> settingsInJson) {
        String deviceNumber = getFromJson(settingsInJson, "gpuDevice", "");
        Map<String, String> devices = GPUAccelerator.INSTANCE.getDevices();
        if (!deviceNumber.isEmpty() && devices.containsValue(deviceNumber)) {
            configEntity.setGpuDevice(deviceNumber);
        } else {
            configEntity.setGpuDevice("");
        }
    }

    private void readServerPort(Map<String, Object> settingsInJson) {
        configEntity.setPort(getFromJson(settingsInJson, "port", 50721));
    }

    private void readCacheNumLimit(Map<String, Object> settingsInJson) {
        configEntity.setCacheNumLimit(getFromJson(settingsInJson, "cacheNumLimit", 1000));
    }

    private void readPriorityFolder(Map<String, Object> settingsInJson) {
        configEntity.setPriorityFolder(getFromJson(settingsInJson, "priorityFolder", ""));
    }

    private void readIgnorePath(Map<String, Object> settingsInJson) {
        String defaultIgnore = "C:\\Windows," + GetWindowsKnownFolder.INSTANCE.getKnownFolder("{AE50C081-EBD2-438A-8655-8A092E34987A}") + ",";
        configEntity.setIgnorePath(getFromJson(settingsInJson, "ignorePath", defaultIgnore));
    }

    private void readUpdateTimeLimit(Map<String, Object> settingsInJson) {
        configEntity.setUpdateTimeLimit(getFromJson(settingsInJson, "updateTimeLimit", 5));
    }

    @SuppressWarnings("unchecked")
    private void readAdvancedConfigs(Map<String, Object> settingsInJson) {
        Map<String, Object> advancedConfigs = (Map<String, Object>) settingsInJson.getOrDefault("advancedConfigs", new HashMap<String, Object>());
        long waitForSearchTasksTimeoutInMills = Long.parseLong(getFromJson(advancedConfigs, "waitForSearchTasksTimeoutInMills", (long) 5 * 60 * 1000).toString());
        boolean isDeleteUsnOnExit = Boolean.parseBoolean(getFromJson(advancedConfigs, "isDeleteUsnOnExit", false).toString());
        long restartMonitorDiskThreadTimeoutInMills = Long.parseLong(getFromJson(advancedConfigs, "restartMonitorDiskThreadTimeoutInMills", (long) 10 * 60 * 1000).toString());
        configEntity.setAdvancedConfigEntity(new AdvancedConfigEntity(
                waitForSearchTasksTimeoutInMills,
                isDeleteUsnOnExit,
                restartMonitorDiskThreadTimeoutInMills)
        );
    }

    private void readSearchThreadNumber(Map<String, Object> settingsInJson) {
        int maxThreadNumber = Runtime.getRuntime().availableProcessors() * 2;
        int searchThreadNumber = getFromJson(settingsInJson, "searchThreadNumber", maxThreadNumber);
        if (searchThreadNumber > maxThreadNumber || searchThreadNumber < 1) {
            searchThreadNumber = maxThreadNumber;
        }
        configEntity.setSearchThreadNumber(searchThreadNumber);
    }

    private String readConfigsJson0() {
        File settings = new File(Constants.CONFIG_FILE);
        if (settings.exists()) {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(settings), StandardCharsets.UTF_8))) {
                String line;
                StringBuilder result = new StringBuilder();
                while (null != (line = br.readLine())) {
                    result.append(line);
                }
                return result.toString();
            } catch (IOException e) {
                log.error(e.getMessage());
            }
        }
        return "";
    }

    /**
     * 打开配置文件，解析为json
     *
     * @return JSON
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> getSettingsJSON() {
        String jsonConfig = readConfigsJson0();
        if (jsonConfig.isEmpty()) {
            return new HashMap<>();
        } else {
            return GsonUtil.INSTANCE.getGson().fromJson(jsonConfig, Map.class);
        }
    }

    /**
     * 尝试从json中读取，若失败则返回默认值
     *
     * @param json       json数据
     * @param key        key
     * @param defaultObj 默认值
     * @return 读取值或默认值
     */
    @SuppressWarnings("unchecked")
    private <T> T getFromJson(Map<String, Object> json, String key, Object defaultObj) {
        if (json == null) {
            return (T) defaultObj;
        }
        Object tmp = json.get(key);
        if (tmp == null) {
            if (IsDebug.isDebug()) {
                log.error("配置文件读取到null值   key : " + key);
            }
            return (T) defaultObj;
        }
        return (T) tmp;
    }

    /**
     * 检查配置并发出警告
     *
     * @param configEntity 配置
     * @return 错误信息
     */
    private static String checkPriorityFolder(ConfigEntity configEntity) {
        String priorityFolder = configEntity.getPriorityFolder();
        if (!priorityFolder.isEmpty() && !Files.exists(Path.of(priorityFolder))) {
            return "Priority folder does not exist";
        }
        return "";
    }

    /**
     * 读取所有配置
     */
    private void readAllSettings() {
        configEntity = new ConfigEntity();
        Map<String, Object> settingsInJson = getSettingsJSON();
        readUpdateTimeLimit(settingsInJson);
        readIgnorePath(settingsInJson);
        readPriorityFolder(settingsInJson);
        readCacheNumLimit(settingsInJson);
        readServerPort(settingsInJson);
        readDisks(settingsInJson);
        readIsEnableGpuAccelerate(settingsInJson);
        readGpuDevice(settingsInJson);
        readSearchThreadNumber(settingsInJson);
        readAdvancedConfigs(settingsInJson);
    }

    /**
     * 将配置保存到文件user/settings.json
     */
    private void saveAllSettings() {
        try (BufferedWriter buffW = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(Constants.CONFIG_FILE), StandardCharsets.UTF_8))) {
            String format = GsonUtil.INSTANCE.getGson().toJson(configEntity);
            buffW.write(format);
        } catch (IOException e) {
            log.error("error: {}", e.getMessage(), e);
        }
    }

    /**
     * 检查configEntity中有没有null值
     *
     * @param config configEntity
     * @return boolean
     */
    private boolean noNullValue(ConfigEntity config) {
        try {
            for (Field field : config.getClass().getDeclaredFields()) {
                field.setAccessible(true);
                Object o = field.get(config);
                if (o == null) {
                    return false;
                }
            }
        } catch (IllegalAccessException e) {
            log.error("error: {}", e.getMessage(), e);
            return false;
        }
        return true;
    }

    /**
     * 修改无效的配置，如线程数为负数等
     *
     * @param config 配置实体
     */
    private void correctInvalidConfigs(ConfigEntity config) {
        if (config.isEnableGpuAccelerate()) {
            config.setEnableGpuAccelerate(GPUAccelerator.INSTANCE.isGPUAvailableOnSystem());
        }
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        int maxThreadNumber = availableProcessors * 2;
        int searchThreadNumber = config.getSearchThreadNumber();
        if (searchThreadNumber > maxThreadNumber || searchThreadNumber < 1) {
            config.setSearchThreadNumber(maxThreadNumber);
        }
    }

    /**
     * 检查配置错误
     *
     * @param event 检查配置事件
     */
    @EventRegister(registerClass = CheckConfigsEvent.class)
    private static void checkConfigsEvent(Event event) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(checkPriorityFolder(getInstance().configEntity));
        if (!stringBuilder.toString().isEmpty()) {
            stringBuilder.append("\n");
        }
        event.setReturnValue(stringBuilder.toString());
    }

    /**
     * 读取所有配置
     *
     * @param event 读取配置事件
     */
    @EventRegister(registerClass = SetConfigsEvent.class)
    private static void setAllConfigsEvent(Event event) {
        SetConfigsEvent setConfigsEvent = (SetConfigsEvent) event;
        AllConfigs allConfigs = getInstance();
        if (setConfigsEvent.getConfigs() == null) {
            // MainClass初始化
            allConfigs.readAllSettings();
            setConfigsEvent.setConfigs(allConfigs.configEntity);
            allConfigs.saveAllSettings();
        } else {
            // 添加高级设置参数
            Map<String, Object> configsJson = allConfigs.getSettingsJSON();
            allConfigs.readAdvancedConfigs(configsJson);
            ConfigEntity tempConfigEntity = setConfigsEvent.getConfigs();
            tempConfigEntity.setAdvancedConfigEntity(allConfigs.configEntity.getAdvancedConfigEntity());

            // 更新设置
            if (allConfigs.noNullValue(tempConfigEntity)) {
                allConfigs.correctInvalidConfigs(tempConfigEntity);
                allConfigs.configEntity = tempConfigEntity;
                allConfigs.saveAllSettings();
            } else {
                throw new NullPointerException("configEntity中有Null值");
            }
        }
    }

    /**
     * 在系统启动后添加一个钩子，在Windows关闭时自动退出
     *
     * @param event 事件
     */
    @EventListener(listenClass = BootSystemEvent.class)
    private static void shutdownListener(Event event) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> EventManagement.getInstance().putEvent(new CloseEvent())));
    }
}
