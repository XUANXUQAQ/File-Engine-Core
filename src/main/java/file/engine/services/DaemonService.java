package file.engine.services;

import file.engine.annotation.EventRegister;
import file.engine.dllInterface.ParentProcessCheck;
import file.engine.event.handler.Event;
import file.engine.event.handler.EventManagement;
import file.engine.event.handler.impl.process.CheckParentProcessEvent;
import file.engine.event.handler.impl.stop.CloseEvent;
import file.engine.utils.ThreadPoolUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@Slf4j
public class DaemonService {

    @EventRegister(registerClass = CheckParentProcessEvent.class)
    public static void checkParentProcess(Event event) {
        EventManagement eventManagement = EventManagement.getInstance();
        ThreadPoolUtil.getInstance().executeTask(() -> {
            while (eventManagement.notMainExit()) {
                boolean parentProcessExist = ParentProcessCheck.INSTANCE.isParentProcessExist();
                if (!parentProcessExist) {
                    log.info("父进程不存在，即将退出");
                    eventManagement.putEvent(new CloseEvent());
                    return;
                }

                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
