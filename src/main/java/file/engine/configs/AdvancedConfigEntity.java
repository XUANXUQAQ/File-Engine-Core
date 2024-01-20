package file.engine.configs;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class AdvancedConfigEntity {

    private long waitForSearchTasksTimeoutInMills;

    private boolean isDeleteUsnOnExit;

    private long restartMonitorDiskThreadTimeoutInMills;
}
