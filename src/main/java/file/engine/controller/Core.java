package file.engine.controller;

import file.engine.annotation.EventListener;
import file.engine.configs.AllConfigs;
import file.engine.configs.ConfigEntity;
import file.engine.configs.Constants;
import file.engine.dllInterface.PathMatcher;
import file.engine.dllInterface.gpu.GPUAccelerator;
import file.engine.entity.SearchInfoEntity;
import file.engine.event.handler.Event;
import file.engine.event.handler.EventManagement;
import file.engine.event.handler.impl.BootSystemEvent;
import file.engine.event.handler.impl.configs.SetConfigsEvent;
import file.engine.event.handler.impl.database.*;
import file.engine.event.handler.impl.stop.CloseEvent;
import file.engine.services.DatabaseService;
import file.engine.utils.RegexUtil;
import file.engine.utils.ThreadPoolUtil;
import file.engine.utils.gson.GsonUtil;
import io.javalin.Javalin;
import io.javalin.http.HttpStatus;
import io.javalin.json.JavalinGson;
import io.javalin.util.JavalinLogger;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Core {

    private static final ConcurrentLinkedQueue<DatabaseService.SearchTask> searchTaskQueue = new ConcurrentLinkedQueue<>();
    private static Javalin server;

    @EventListener(listenClass = BootSystemEvent.class)
    private static void startServer(Event event) {
        JavalinLogger.enabled = false;
        var databaseService = DatabaseService.getInstance();
        var eventManager = EventManagement.getInstance();
        var app = Javalin.create(config -> config.jsonMapper(new JavalinGson(GsonUtil.INSTANCE.getGson(), true)))
                .exception(Exception.class, (e, ctx) -> log.error("error {}, ", e.getMessage(), e))
                .error(HttpStatus.NOT_FOUND, ctx -> ctx.json("not found"))
                .get("/config", ctx -> ctx.json(AllConfigs.getInstance().getConfigEntity()))
                .get("/gpu", ctx -> ctx.json(GPUAccelerator.INSTANCE.getDevices()))
                .post("/config", ctx -> eventManager.putEvent(new SetConfigsEvent(ctx.bodyAsClass(ConfigEntity.class))))
                .post("/close", ctx -> eventManager.putEvent(new CloseEvent()))
                .delete("/closeConnections", ctx -> PathMatcher.INSTANCE.closeConnections())
                .get("/status", ctx -> ctx.result(databaseService.getStatus().toString()))
                // db control
                .post("/flushFileChanges", ctx -> eventManager.putEvent(new FlushFileChangesEvent()))
                .post("/optimize", ctx -> eventManager.putEvent(new OptimizeDatabaseEvent()))
                // search
                .get("/frequentResult", ctx -> ctx.json(databaseService.getFrequentlyUsedCaches(Integer.parseInt(Objects.requireNonNull(ctx.queryParam("num"))))))
                .post("/search", ctx -> {
                    StartSearchEvent startSearchEvent = new StartSearchEvent(
                            generateSearchKeywordsAndSearchCase(Objects.requireNonNull(ctx.queryParam("searchText")),
                                    Integer.parseInt(Objects.requireNonNull(ctx.queryParam("maxResultNum"))))
                    );
                    var ref = new Object() {
                        Object retVal;
                    };
                    eventManager.putEvent(startSearchEvent, successEvent -> successEvent.getReturnValue().ifPresent(o -> {
                        var searchTask = (DatabaseService.SearchTask) o;
                        final long startTime = System.currentTimeMillis();
                        while (!searchTask.isSearchDone() && System.currentTimeMillis() - startTime < Constants.MAX_TASK_EXIST_TIME) {
                            try {
                                TimeUnit.MILLISECONDS.sleep(50);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        LinkedHashSet<String> ret = new LinkedHashSet<>();
                        ret.addAll(searchTask.getCacheAndPriorityResults());
                        ret.addAll(searchTask.getTempResults());
                        ref.retVal = ret;
                    }), errorEvent -> ref.retVal = Collections.emptySet());
                    eventManager.waitForEvent(startSearchEvent);
                    ctx.json(ref.retVal);
                })
                .post("/prepareSearch", ctx -> {
                    PrepareSearchEvent prepareSearchEvent = new PrepareSearchEvent(
                            generateSearchKeywordsAndSearchCase(Objects.requireNonNull(ctx.queryParam("searchText")),
                                    Integer.parseInt(Objects.requireNonNull(ctx.queryParam("maxResultNum"))))
                    );
                    var ref = new Object() {
                        String ret;
                    };
                    eventManager.putEvent(prepareSearchEvent, successEvent -> successEvent.getReturnValue().ifPresent(o -> {
                        DatabaseService.SearchTask searchTask = (DatabaseService.SearchTask) o;
                        searchTaskQueue.offer(searchTask);
                        ref.ret = searchTask.getUuid().toString();
                    }), errorEvent -> {
                        StringBuilder stringBuilder = new StringBuilder();
                        stringBuilder.append("failed: ");
                        errorEvent.getException().ifPresent(ex -> stringBuilder.append(ex.getMessage()));
                        ref.ret = stringBuilder.toString();
                    });
                    eventManager.waitForEvent(prepareSearchEvent);
                    ctx.json(ref.ret);
                })
                .post("/searchAsync", ctx -> {
                    StartSearchEvent startSearchEvent = new StartSearchEvent(
                            generateSearchKeywordsAndSearchCase(Objects.requireNonNull(ctx.queryParam("searchText")),
                                    Integer.parseInt(Objects.requireNonNull(ctx.queryParam("maxResultNum"))))
                    );
                    var ref = new Object() {
                        String ret;
                    };
                    eventManager.putEvent(startSearchEvent, successEvent -> successEvent.getReturnValue().ifPresent(o -> {
                        DatabaseService.SearchTask searchTask = (DatabaseService.SearchTask) o;
                        searchTaskQueue.offer(searchTask);
                        ref.ret = searchTask.getUuid().toString();
                    }), error -> {
                        StringBuilder stringBuilder = new StringBuilder();
                        stringBuilder.append("failed: ");
                        error.getException().ifPresent(ex -> stringBuilder.append(ex.getMessage()));
                        ref.ret = stringBuilder.toString();
                    });
                    eventManager.waitForEvent(startSearchEvent);
                    ctx.json(ref.ret);
                })
                .delete("/search", ctx -> eventManager.putEvent(new StopSearchEvent()))
                .get("/cacheResult", ctx -> ctx.json(
                        getSearchCacheResults(Integer.parseInt(Objects.requireNonNull(ctx.queryParam("startIndex"))), ctx.queryParam("uuid"))
                ))
                .get("/result", ctx -> ctx.json(
                        getSearchResults(Integer.parseInt(Objects.requireNonNull(ctx.queryParam("startIndex"))), ctx.queryParam("uuid"))
                ))
                .delete("/result", ctx -> ctx.result(
                        String.valueOf(searchTaskQueue.removeIf(searchTask -> Objects.equals(searchTask.getUuid().toString(), ctx.queryParam("uuid"))))
                ))
                // cache
                .post("/cache", ctx -> eventManager.putEvent(new AddToCacheEvent(ctx.queryParam("path"))))
                .get("/cache", ctx -> ctx.json(databaseService.getCache()))
                .delete("/cache", ctx -> eventManager.putEvent(new DeleteFromCacheEvent(ctx.queryParam("path"))))
                // index
                .post("/update", ctx -> eventManager.putEvent(new UpdateDatabaseEvent(Boolean.parseBoolean(ctx.queryParam("isDropPrevious")))))
                // suffix priority
                .post("/suffixPriority", ctx -> eventManager.putEvent(new AddToSuffixPriorityMapEvent(
                        ctx.queryParam("suffix"), Integer.parseInt(Objects.requireNonNull(ctx.queryParam("priority")))
                )))
                .delete("/suffixPriority", ctx -> eventManager.putEvent(new DeleteFromSuffixPriorityMapEvent(ctx.queryParam("suffix"))))
                .get("/suffixPriority", ctx -> ctx.json(databaseService.getPriorityMap()))
                .put("/suffixPriority", ctx -> eventManager.putEvent(new UpdateSuffixPriorityEvent(
                        ctx.queryParam("oldSuffix"), ctx.queryParam("newSuffix"), Integer.parseInt(Objects.requireNonNull(ctx.queryParam("priority")))
                )))
                .delete("/clearSuffixPriority", ctx -> eventManager.putEvent(new ClearSuffixPriorityMapEvent()))
                .get("/version", ctx -> ctx.result(AllConfigs.getVersion()))
                .get("/buildVersion", ctx -> ctx.result(AllConfigs.getBuildVersion()));
        server = app;
        app.start(((BootSystemEvent) event).port);
        startClearTaskThread();
    }

    @EventListener(listenClass = CloseEvent.class)
    private static void close(Event event) {
        if (server != null) {
            server.stop();
        }
    }

    private static void startClearTaskThread() {
        ThreadPoolUtil.getInstance().executeTask(() -> {
            EventManagement eventManagement = EventManagement.getInstance();
            long startCheckTime = System.currentTimeMillis();
            while (eventManagement.notMainExit()) {
                if (System.currentTimeMillis() - startCheckTime > Constants.MAX_TASK_EXIST_TIME) {
                    startCheckTime = System.currentTimeMillis();
                    ArrayList<DatabaseService.SearchTask> taskToRemove = new ArrayList<>();
                    searchTaskQueue.forEach(searchTask -> {
                        if (System.currentTimeMillis() - searchTask.getTaskCreateTimeMills() > Constants.MAX_TASK_EXIST_TIME) {
                            taskToRemove.add(searchTask);
                        }
                    });
                    searchTaskQueue.removeAll(taskToRemove);
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private static HashMap<String, Object> getSearchResults(int startIndex, String uuid) {
        HashMap<String, Object> retWrapper = new HashMap<>();
        getSearchTaskByUUID(uuid).ifPresent(currentSearchTask -> {
            var tempResults = currentSearchTask.getTempResults();
            genSearchResultMap(startIndex, currentSearchTask, retWrapper, tempResults);
        });
        return retWrapper;
    }

    private static HashMap<String, Object> getSearchCacheResults(int startIndex, String uuid) {
        HashMap<String, Object> ret = new HashMap<>();
        getSearchTaskByUUID(uuid).ifPresent(currentSearchTask -> {
            var cacheAndPriorityResults = currentSearchTask.getCacheAndPriorityResults();
            genSearchResultMap(startIndex, currentSearchTask, ret, cacheAndPriorityResults);
        });
        return ret;
    }

    private static Optional<DatabaseService.SearchTask> getSearchTaskByUUID(String uuid) {
        DatabaseService.SearchTask currentSearchTask;
        if (uuid == null) {
            currentSearchTask = searchTaskQueue.peek();
        } else {
            var tmpList = searchTaskQueue.stream()
                    .filter(searchTask -> uuid.equals(searchTask.getUuid().toString()))
                    .toList();
            if (tmpList.isEmpty()) {
                currentSearchTask = null;
            } else {
                currentSearchTask = tmpList.get(0);
            }
        }
        return Optional.ofNullable(currentSearchTask);
    }

    private static void genSearchResultMap(int startIndex,
                                           DatabaseService.SearchTask searchTask,
                                           HashMap<String, Object> retWrapper,
                                           ConcurrentLinkedQueue<String> resultsContainer) {
        retWrapper.put("uuid", searchTask.getUuid().toString());
        ConcurrentLinkedQueue<String> list = new ConcurrentLinkedQueue<>();
        if (startIndex != 0) {
            int i = 0;
            for (String e : resultsContainer) {
                if (i >= startIndex) {
                    list.add(e);
                }
                ++i;
            }
        } else {
            list.addAll(resultsContainer);
        }
        retWrapper.put("data", list);
        retWrapper.put("nextIndex", list.size() + startIndex);
        retWrapper.put("isDone", searchTask.isSearchDone());
    }

    /**
     * 根据用户输入设置搜索关键字
     */
    private static SearchInfoEntity generateSearchKeywordsAndSearchCase(String searchBarText, int maxResultNum) {
        String searchText;
        String[] searchCase;
        String[] keywords;
        if (!searchBarText.isEmpty()) {
            final int i = searchBarText.lastIndexOf('|');
            if (i == -1) {
                searchCase = null;
                searchText = searchBarText;
            } else {
                searchText = searchBarText.substring(0, i);
                var searchCaseStr = searchBarText.substring(i + 1);
                if (!searchCaseStr.isEmpty()) {
                    String[] tmpSearchCase = RegexUtil.semicolon.split(searchCaseStr);
                    searchCase = new String[tmpSearchCase.length];
                    for (int j = 0; j < tmpSearchCase.length; j++) {
                        searchCase[j] = tmpSearchCase[j].trim();
                    }
                } else {
                    searchCase = null;
                }
            }
            keywords = RegexUtil.semicolon.split(searchText);
        } else {
            keywords = null;
            searchCase = null;
            searchText = "";
        }
        return new SearchInfoEntity(() -> searchText, () -> searchCase, () -> keywords, maxResultNum);
    }
}
