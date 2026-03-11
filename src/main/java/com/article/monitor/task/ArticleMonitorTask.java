package com.article.monitor.task;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.RuntimeUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.extra.mail.MailUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 文章消费监控核心任务
 * * 严格按照阿里 Java 开发规范编写。
 * 包含增量信源监控、指定端口健康容灾检查、断流及失败率预警。
 * * 已引入并发安全的 Atomic 原子类彻底解决多线程调度下的并发写问题。
 *
 * @version 7.0 (全量任务执行结果日志回显版，正常打日志，异常才发邮件)
 */
@Component
@Slf4j
public class ArticleMonitorTask {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    // =========================================================================
    // 环境变量注入 (Configuration Injection)
    // =========================================================================

    @Value("${monitor.alarm.email-to}")
    private String targetEmail;

    @Value("${monitor.server.ip}")
    private String serverIp;

    @Value("${monitor.server.ports}")
    private String serverPorts;

    @Value("${monitor.kafka.script-path}")
    private String kafkaScriptPath;

    // --- 动态阈值注入 (从 application.yml 中读取) ---
    @Value("${monitor.alarm.daily-limit.port}")
    private int portAlarmDailyLimit;

    @Value("${monitor.alarm.daily-limit.kafka}")
    private int kafkaAlarmDailyLimit;

    @Value("${monitor.rules.data-stop-hours}")
    private int dataStopThresholdHours;

    @Value("${monitor.rules.fail-level.l1}")
    private int failLevelL1;

    @Value("${monitor.rules.fail-level.l2}")
    private int failLevelL2;

    @Value("${monitor.rules.fail-level.l3}")
    private int failLevelL3;

    // =========================================================================
    // 静态常量 (Static Constants)
    // =========================================================================

    /** 异常信息匹配关键字：资源链接超过数据库字段长度限制 */
    private static final String ERR_MSG_LINK_TOO_LONG = "资源链接超过字段长度：8255";

    /** Socket 端口探测的超时时间（毫秒），避免线程长期阻塞 */
    private static final int SOCKET_TIMEOUT_MS = 3000;

    /** 增量监测基准 ID。使用 volatile 保证多线程可见性 */
    private static volatile Long currentBaseId = null;

    // =========================================================================
    // 实例变量 (Instance Variables) - 运行状态记录锁 (均使用并发安全原子类)
    // =========================================================================

    /** 记录最近一次探测到新数据的时间，用于推算断流时长 */
    private LocalDateTime lastSeenDataTime;

    /** 记录今日已发送的端口告警总次数，零点清零 */
    private final AtomicInteger dailyPortAlarmCount = new AtomicInteger(0);

    /** 记录今日已触发的最高失败率告警级别，避免同级别重复发信，零点清零 */
    private final AtomicInteger lastReportedLevel = new AtomicInteger(0);

    /** 断流告警状态锁：今天报过警就不再报，防止半小时轰炸一次 */
    private final AtomicBoolean isDataFlowAlarmed = new AtomicBoolean(false);

    /** Kafka脚本告警次数限制记录，零点清零 */
    private final AtomicInteger dailyKafkaAlarmCount = new AtomicInteger(0);

    // =========================================================================
    // 初始化与监控任务逻辑
    // =========================================================================

    @PostConstruct
    public void init() {
        this.lastSeenDataTime = LocalDateTime.now();
        initBaseIdIfNull();
        log.info(">>>> [System_Init] 监控任务初始化完成. 当前 BaseId: {}", currentBaseId);

        // 启动后异步延迟 3 秒执行自检，完美避开 Tomcat 启动瞬间的端口占用冲突
        CompletableFuture.runAsync(() -> {
            try { Thread.sleep(3000); } catch (InterruptedException ignored) {}
            log.info(">>>> [System_Init] 正在执行启动后的首次端口状态自检...");
            this.runInitialPortCheck();
        });
    }

    private void runInitialPortCheck() {
        String[] portsArray = serverPorts.split(",");
        for (String p : portsArray) {
            int port = Integer.parseInt(p.trim());
            boolean isOpen = checkPortOpen(port);
            if (isOpen) {
                log.info("✅ [Init_Check] 端口 {} 状态正常 (OPEN)", port);
            } else {
                log.error("❌ [Init_Check] 端口 {} 状态异常 (CLOSED)! 请检查服务是否启动", port);
            }
        }
    }

    private void initBaseIdIfNull() {
        if (currentBaseId == null) {
            try {
                String sql = "SELECT MAX(id) FROM trs_datasource";
                Long maxId = jdbcTemplate.queryForObject(sql, Long.class);
                currentBaseId = (maxId != null) ? maxId : 0L;
            } catch (Exception e) {
                log.error(">>>> [System_Init] 获取最大基准ID失败，默认降级为 0", e);
                currentBaseId = 0L;
            }
        }
    }

    /**
     * 1. 增量信源监测
     */
    @Scheduled(cron = "0 0/30 * * * ?")
    public void monitorNewSources() {
        initBaseIdIfNull();
        log.info(">>>> [Task_Start] 开始执行增量信源检查...");

        try {
            String sql = "SELECT id, mediaName FROM trs_datasource WHERE id > ? ORDER BY id DESC";
            List<Map<String, Object>> newSources = jdbcTemplate.queryForList(sql, currentBaseId);

            if (CollUtil.isNotEmpty(newSources)) {
                StringBuilder content = new StringBuilder("系统检测到新增信源，列表如下：<br/><br/>");
                for (Map<String, Object> source : newSources) {
                    content.append("ID: ").append(source.get("id"))
                            .append(" | 名称: ").append(source.get("mediaName")).append("<br/>");
                }

                log.warn("⚠️ [Check_Result] 发现 {} 个新信源，准备发送告警邮件...", newSources.size());
                sendEmailAlarm("【通知】系统新增信源告警", content.toString());

                currentBaseId = Long.parseLong(newSources.get(0).get("id").toString());
                log.info(">>>> [BaseId_Update] 基准ID已更新至: {}", currentBaseId);
            } else {
                // --- 新增：正常状态回显 ---
                log.info("✅ [Check_Result] 增量信源检查完毕: 未发现新信源, 当前 BaseId 维持在 {}", currentBaseId);
            }
        } catch (Exception e) {
            log.error("❌ [Source_Monitor_Error] 增量信源检查失败", e);
        }
    }

    /**
     * 2. 多端口连接探测
     */
    @Scheduled(fixedRate = 300000)
    public void monitorServerPorts() {
        log.info(">>>> [Heartbeat] 开始执行端口健康检查任务...");

        if (dailyPortAlarmCount.get() >= portAlarmDailyLimit) {
            log.warn(">>>> [Skip] 端口告警已达今日上限，跳过探测.");
            return;
        }

        String[] portsArray = serverPorts.split(",");
        int portKafka = Integer.parseInt(portsArray[0].trim());
        int portTrs = Integer.parseInt(portsArray[1].trim());
        int portConsumer = Integer.parseInt(portsArray[2].trim());

        boolean kafkaOk = checkPortOpen(portKafka);
        boolean trsOk = checkPortOpen(portTrs);
        boolean consumerOk = checkPortOpen(portConsumer);

        List<String> success = new ArrayList<>();
        if (kafkaOk) success.add(String.valueOf(portKafka));
        if (trsOk) success.add(String.valueOf(portTrs));
        if (consumerOk) success.add(String.valueOf(portConsumer));

        List<String> errors = new ArrayList<>();
        if (!kafkaOk) errors.add("Kafka服务 (" + portKafka + ") 失联");
        if (!trsOk && !consumerOk) errors.add("数据流服务容灾链路异常 (TRS " + portTrs + " 与 消费端 " + portConsumer + " 均无法连接)");

        if (errors.isEmpty()) {
            log.info("✅ [Check_Result] 端口健康检查通过! 正常状态端口: [{}]", String.join(", ", success));
        } else {
            log.warn("⚠️ [Check_Result] 端口健康检查异常! 正常: [{}], 告警内容: {}", String.join(", ", success), errors);
            sendEmailAlarm("【紧急】核心服务端口异常", "检测到以下故障: <br/>" + String.join("<br/>", errors));
            dailyPortAlarmCount.incrementAndGet();
        }
    }

    /**
     * 3. 数据流断流监测
     */
    @Scheduled(cron = "0 0/30 * * * ?")
    public void monitorDataFlow() {
        log.info(">>>> [Task_Start] 开始执行数据流断流监测...");
        try {
            LocalDateTime oneHourAgo = LocalDateTime.now().minusHours(1);
            String sql = "SELECT COUNT(*) FROM article WHERE createTime >= ?";
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, oneHourAgo);

            if (count != null && count > 0) {
                lastSeenDataTime = LocalDateTime.now();
                if (isDataFlowAlarmed.get()) {
                    isDataFlowAlarmed.set(false);
                    log.info("✅ [Check_Result] 数据恢复入库，解除断流锁定。最近1小时入库量: {}", count);
                } else {
                    // --- 新增：正常状态回显 ---
                    log.info("✅ [Check_Result] 数据流活性正常: 最近1小时内有 {} 条新数据入库", count);
                }
            } else {
                long hours = Duration.between(lastSeenDataTime, LocalDateTime.now()).toHours();
                log.warn("⚠️ [Check_Result] 数据流异常: 已连续 {} 小时无新数据入库", hours);

                if (hours >= dataStopThresholdHours) {
                    if (!isDataFlowAlarmed.get()) {
                        sendEmailAlarm("【警告】数据断流预警", "服务器 " + serverIp + " 已连续 " + hours + " 小时无新数据入库。");
                        isDataFlowAlarmed.set(true);
                    }
                }
            }
        } catch (Exception e) {
            log.error("❌ [DataFlow_Error] 活性检测失败", e);
        }
    }

    /**
     * 4. 入库失败率阶梯告警
     */
    @Scheduled(cron = "0 0/30 * * * ?")
    public void monitorFailureRate() {
        log.info(">>>> [Task_Start] 开始统计入库失败率...");
        try {
            String sql = "SELECT COUNT(*) FROM article WHERE createTime >= CURDATE() AND isVideoTranscod = 3 AND resourceUrl != ?";
            Integer failCount = jdbcTemplate.queryForObject(sql, Integer.class, ERR_MSG_LINK_TOO_LONG);

            if (failCount != null) {
                // --- 新增：正常状态回显 ---
                log.info("✅ [Check_Result] 失败率统计完成: 今日累计超长异常失败 {} 条", failCount);

                int level = (failCount >= failLevelL3) ? 3 : (failCount >= failLevelL2 ? 2 : (failCount >= failLevelL1 ? 1 : 0));

                if (level > lastReportedLevel.get()) {
                    log.warn("⚠️ [FailRate_Alarm] 失败率跨越阈值，触发 L{} 级别告警邮件!", level);
                    sendEmailAlarm("【告警升级】入库失败率达到 L" + level, "今日累计失败已达: " + failCount + " 条。");
                    lastReportedLevel.set(level);
                }
            }
        } catch (Exception e) {
            log.error("❌ [FailureRate_Error] 统计失败", e);
        }
    }

    /**
     * 5. Kafka 脚本健康监测
     */
    @Scheduled(cron = "0 0/30 * * * ?")
    public void monitorKafkaHealth() {
        if (StrUtil.isBlank(kafkaScriptPath)) return;

        log.info(">>>> [Task_Start] 开始调用 Kafka 健康监测脚本...");

        try {
            String result = RuntimeUtil.execForStr(kafkaScriptPath);
            if (StrUtil.containsAnyIgnoreCase(result, "Exception", "Error", "fail")) {
                log.warn("⚠️ [Check_Result] Kafka脚本执行异常，发现错误关键字！");
                if (dailyKafkaAlarmCount.get() < kafkaAlarmDailyLimit) {
                    sendEmailAlarm("【异常】Kafka 监控脚本错误", "执行返回异常信息:<br/>" + result);
                    dailyKafkaAlarmCount.incrementAndGet();
                }
            } else {
                // --- 新增：正常状态回显 ---
                log.info("✅ [Check_Result] Kafka脚本监控通过: 状态正常");
            }
        } catch (Exception e) {
            log.error("❌ [Check_Result] Kafka脚本执行彻底失败: {}", e.getMessage());
            if (dailyKafkaAlarmCount.get() < kafkaAlarmDailyLimit) {
                sendEmailAlarm("【错误】Kafka 脚本执行失败", "路径: " + kafkaScriptPath + "<br/>异常: " + e.getMessage());
                dailyKafkaAlarmCount.incrementAndGet();
            }
        }
    }

    /**
     * 6. 零点状态重置
     */
    @Scheduled(cron = "0 0 0 * * ?")
    public void resetDailyCounters() {
        dailyPortAlarmCount.set(0);
        lastReportedLevel.set(0);
        isDataFlowAlarmed.set(false);
        dailyKafkaAlarmCount.set(0);

        initBaseIdIfNull();
        log.info(">>>> [Maintenance_Reset] 零点计数器及状态锁重置完成。");
    }

    // =========================================================================
    // 内部私有工具方法
    // =========================================================================

    private boolean checkPortOpen(int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(serverIp, port), SOCKET_TIMEOUT_MS);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private void sendEmailAlarm(String title, String content) {
        try {
            MailUtil.send(targetEmail, title, content, true);
            log.info(">>>> [Email_Sent] 告警邮件已发送: {}", title);
        } catch (Exception e) {
            log.error(">>>> [Email_Failed] 邮件发送失败! 标题: {}", title, e);
        }
    }
}
