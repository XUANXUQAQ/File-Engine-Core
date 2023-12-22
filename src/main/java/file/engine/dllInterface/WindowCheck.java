package file.engine.dllInterface;

import java.nio.file.Path;

public enum WindowCheck {
    INSTANCE;

    static {
        System.load(Path.of("user/windowCheck.dll").toAbsolutePath().toString());
    }

    /**
     * 判断当前是否有全屏任务，如游戏全屏，全屏看电影等
     *
     * @return true如果有
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public native boolean isForegroundFullscreen();
}
