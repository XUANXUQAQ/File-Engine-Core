package file.engine.dllInterface;

import java.nio.file.Path;

public enum ParentProcessCheck {
    INSTANCE;

    static {
        System.load(Path.of("parentProcessCheck.dll").toAbsolutePath().toString());
    }

    /**
     * 判断父进程是否存在
     *
     * @return true如果父进程存在
     */
    public native boolean isParentProcessExist();
}
