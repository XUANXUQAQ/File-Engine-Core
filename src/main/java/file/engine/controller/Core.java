package file.engine.controller;

import file.engine.annotation.EventListener;
import file.engine.configs.AllConfigs;
import file.engine.configs.ConfigEntity;
import file.engine.configs.Constants;
import file.engine.entity.SearchInfoEntity;
import file.engine.event.handler.Event;
import file.engine.event.handler.EventManagement;
import file.engine.event.handler.impl.BootSystemEvent;
import file.engine.event.handler.impl.configs.SetConfigsEvent;
import file.engine.event.handler.impl.database.*;
import file.engine.event.handler.impl.stop.CloseEvent;
import file.engine.services.DatabaseService;
import file.engine.utils.RegexUtil;
import file.engine.utils.gson.GsonUtil;
import io.javalin.Javalin;
import io.javalin.http.HttpStatus;
import io.javalin.json.JavalinGson;
import io.javalin.util.JavalinLogger;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Core {

    private static volatile DatabaseService.SearchTask currentSearchTask;
    private static Javalin server;

    @EventListener(listenClass = BootSystemEvent.class)
    private static void startServer(Event event) {
        JavalinLogger.enabled = false;
        var allConfigs = AllConfigs.getInstance();
        var databaseService = DatabaseService.getInstance();
        var eventManager = EventManagement.getInstance();
        var app = Javalin.create(config -> config.jsonMapper(new JavalinGson(GsonUtil.INSTANCE.getGson())))
                .exception(Exception.class, (e, ctx) -> log.error("error {}, ", e.getMessage(), e))
                .error(HttpStatus.NOT_FOUND, ctx -> ctx.json("not found"))
                .post("/config", ctx -> eventManager.putEvent(new SetConfigsEvent(ctx.bodyAsClass(ConfigEntity.class))))
                .post("/close", ctx -> eventManager.putEvent(new CloseEvent()))
                .get("/status", ctx -> ctx.result(databaseService.getStatus().toString()))
                // db control
                .post("/flushFileChanges", ctx -> eventManager.putEvent(new FlushFileChangesEvent()))
                .post("/checkDbEmpty", ctx -> eventManager.putEvent(new CheckDatabaseEmptyEvent()))
                .post("/optimize", ctx -> eventManager.putEvent(new OptimizeDatabaseEvent()))
                // search
                .get("/frequentResult", ctx -> ctx.json(databaseService.getFrequentlyUsedCaches(Integer.parseInt(Objects.requireNonNull(ctx.queryParam("num"))))))
                .post("/search", ctx -> {
                    final DatabaseService.SearchTask[] searchTask = new DatabaseService.SearchTask[1];
                    StartSearchEvent startSearchEvent = new StartSearchEvent(
                            generateSearchKeywordsAndSearchCase(Objects.requireNonNull(ctx.queryParam("searchText")),
                                    Integer.parseInt(Objects.requireNonNull(ctx.queryParam("maxResultNum"))))
                    );
                    eventManager.putEvent(startSearchEvent);
                    eventManager.waitForEvent(startSearchEvent);
                    startSearchEvent.getReturnValue().ifPresent(o -> searchTask[0] = (DatabaseService.SearchTask) o);
                    final long startTime = System.currentTimeMillis();
                    while (!searchTask[0].isSearchDone() && System.currentTimeMillis() - startTime < Constants.MAX_TASK_EXIST_TIME) {
                        TimeUnit.MILLISECONDS.sleep(50);
                    }
                    LinkedHashSet<String> ret = new LinkedHashSet<>();
                    ret.addAll(searchTask[0].getCacheAndPriorityResults());
                    ret.addAll(searchTask[0].getTempResults());
                    ctx.json(ret);
                })
                .post("/prepareSearch", ctx -> {
                    PrepareSearchEvent prepareSearchEvent = new PrepareSearchEvent(
                            generateSearchKeywordsAndSearchCase(Objects.requireNonNull(ctx.queryParam("searchText")),
                                    Integer.parseInt(Objects.requireNonNull(ctx.queryParam("maxResultNum"))))
                    );
                    eventManager.putEvent(prepareSearchEvent);
                    eventManager.waitForEvent(prepareSearchEvent);
                    prepareSearchEvent.getReturnValue().ifPresent(o -> {
                        DatabaseService.SearchTask searchTask = (DatabaseService.SearchTask) o;
                        currentSearchTask = searchTask;
                        ctx.result(searchTask.getUuid().toString());
                    });
                })
                .post("/searchAsync", ctx -> {
                    StartSearchEvent startSearchEvent = new StartSearchEvent(
                            generateSearchKeywordsAndSearchCase(Objects.requireNonNull(ctx.queryParam("searchText")),
                                    Integer.parseInt(Objects.requireNonNull(ctx.queryParam("maxResultNum"))))
                    );
                    eventManager.putEvent(startSearchEvent);
                    eventManager.waitForEvent(startSearchEvent);
                    startSearchEvent.getReturnValue().ifPresent(o -> {
                        DatabaseService.SearchTask searchTask = (DatabaseService.SearchTask) o;
                        currentSearchTask = searchTask;
                        ctx.result(searchTask.getUuid().toString());
                    });
                })
                .delete("/search", ctx -> eventManager.putEvent(new StopSearchEvent()))
                .get("/cacheResult", ctx -> ctx.json(getSearchCacheResults()))
                .get("/result", ctx -> ctx.json(getSearchResults()))
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
                .delete("/clearSuffixPriority", ctx -> eventManager.putEvent(new ClearSuffixPriorityMapEvent()));
        server = app;
        app.start(allConfigs.getConfigEntity().getPort());
    }

    @EventListener(listenClass = CloseEvent.class)
    private static void close(Event event) {
        if (server != null) {
            server.close();
        }
    }

    private static HashMap<String, Object> getSearchResults() {
        HashMap<String, Object> retWrapper = new HashMap<>();
        if (currentSearchTask != null) {
            LinkedHashSet<String> ret = new LinkedHashSet<>();
            ret.addAll(currentSearchTask.getCacheAndPriorityResults());
            ret.addAll(currentSearchTask.getTempResults());
            retWrapper.put("uuid", currentSearchTask.getUuid().toString());
            retWrapper.put("data", ret);
        }
        return retWrapper;
    }

    private static HashMap<String, Object> getSearchCacheResults() {
        HashMap<String, Object> ret = new HashMap<>();
        if (currentSearchTask != null) {
            ret.put("uuid", currentSearchTask.getUuid().toString());
            ret.put("data", currentSearchTask.getCacheAndPriorityResults());
        }
        return ret;
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
