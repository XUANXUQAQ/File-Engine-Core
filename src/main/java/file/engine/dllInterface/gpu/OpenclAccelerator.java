package file.engine.dllInterface.gpu;

import java.nio.file.Path;

enum OpenclAccelerator implements IGPUAccelerator {
    INSTANCE;

    private volatile boolean isOpenclLoaded;

    OpenclAccelerator() {
        try {
            System.load(Path.of("user/openclAccelerator.dll").toAbsolutePath().toString());
            isOpenclLoaded = true;
        } catch (UnsatisfiedLinkError | Exception e) {
            isOpenclLoaded = false;
        }
    }

    public native void resetAllResultStatus();

    public native String[] match(String[] searchCase,
                                 boolean isIgnoreCase,
                                 String searchText,
                                 String[] keywords,
                                 String[] keywordsLowerCase,
                                 boolean[] isKeywordPath,
                                 int maxResultNumber,
                                 int resultCollectThreadNum);

    public boolean isGPUAvailableOnSystem() {
        if (isOpenclLoaded) {
            return isOpenCLAvailable();
        }
        return false;
    }

    private native boolean isOpenCLAvailable();

    public native boolean isMatchDone(String key);

    public native int matchedNumber(String key);

    public native void stopCollectResults();

    public native boolean hasCache();

    public native boolean isCacheExist(String key);

    public native void initCache(String key, String[] records);

    public native void addRecordsToCache(String key, Object[] records);

    public native void removeRecordsFromCache(String key, Object[] records);

    public native void clearCache(String key);

    public native void clearAllCache();

    public native boolean isCacheValid(String key);

    public native int getGPUMemUsage();

    public native void initialize();

    public native void release();

    public native String[] getDevices();

    public native boolean setDevice(int deviceNum);
}