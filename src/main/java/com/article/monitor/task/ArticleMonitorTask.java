package com.article.monitor.task;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.extra.mail.MailUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * 文章消费监控核心任务
 * <p>
 * 功能：
 * 1. 增量信源监测（动态基准ID）
 * 2. 服务器核心端口连通性监测
 * 3. 数据流断流预警
 * 4. 入库失败率阶梯告警
 * </p>
 *
 * @author Gemini Expert
 * @date 2026-02-12
 */
@Component
@Slf4j
public class ArticleMonitorTask {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    // --- 环境配置注入 ---

    @Value("${monitor.alarm.email-to}")
    private String targetEmail;

    @Value("${monitor.server.ip}")
    private String serverIp;

    @Value("${monitor.server.ports}")
    private String serverPorts;

    @Value("${monitor.kafka.script-path}")
    private String kafkaScriptPath;

    // --- 静态常量定义 (Static Constants) ---
    // 按照代码规范，常量命名使用全大写+下划线，且必须有注释说明用途

    /**
     * 数据库中资源链接字段的长度限制错误提示信息。
     * 用于过滤因字段长度不足导致的非系统性故障。
     */
    private static final String ERR_MSG_LINK_TOO_LONG = "资源链接超过字段长度：8255";

    /**
     * 数据断流阈值（小时）。
     * 当超过此时间没有新数据入库时，触发严重告警。
     */
    private static final int DATA_STOP_THRESHOLD_HOURS = 8;

    /**
     * 端口告警每日上限次数。
     * 防止因网络波动导致邮件轰炸，每天最多发送5次端口故障邮件。
     */
    private static final int PORT_ALARM_DAILY_LIMIT = 5;

    /**
     * Socket连接超时时间（毫秒）。
     * 3秒无法建立连接即视为端口不通。
     */
    private static final int SOCKET_TIMEOUT_MS = 3000;

    /**
     * 一级告警阈值（Level 1）。
     * 当日失败数达到20条触发。
     */
    private static final int FAIL_LEVEL_L1 = 20;

    /**
     * 二级告警阈值（Level 2）。
     * 当日失败数达到50条触发。
     */
    private static final int FAIL_LEVEL_L2 = 50;

    /**
     * 三级告警阈值（Level 3）。
     * 当日失败数达到100条触发，属于严重级别。
     */
    private static final int FAIL_LEVEL_L3 = 100;

    // --- 内部状态变量 (Instance Variables) ---
    // 这些变量维护了单例Bean的运行时状态，非静态，随应用生命周期存在

    /**
     * 上一次监测到有数据入库的时间点。
     * 初始化为应用启动时间。
     */
    private LocalDateTime lastSeenDataTime;

    /**
     * 今日已发送端口告警的次数。
     * 每天0点重置。
     */
    private int dailyPortAlarmCount = 0;

    /**
     * 今日已上报的最高故障等级。
     * 用于阶梯告警，防止同一级别重复发送。每天0点重置。
     */
    private int lastReportedLevel = 0;

    /**
     * 当前信源基准ID。
     * 核心逻辑：
     * 1. 初始为 null。
     * 2. 首次运行查询数据库 MAX(id) 赋值，不发邮件。
     * 3. 后续运行查询 > currentBaseId 的数据，发邮件并更新。
     * 使用 volatile 保证多线程（如果有）可见性，尽管 Scheduled 默认单线程。
     */
    private volatile Long currentBaseId = null;

    @PostConstruct
    public void init() {
        this.lastSeenDataTime = LocalDateTime.now();
        log.info(">>>> [System_Init] 监控任务初始化完成. 目标服务器: {}, 监测端口: {}", serverIp, serverPorts);
    }

    /**
     * 1. 增量信源明细推送
     * <p>
     * 逻辑：
     * - 如果 currentBaseId 为空，先查库初始化（同步当前状态）。
     * - 如果不为空，查询比它大的 ID，如果有则发送邮件并更新 currentBaseId。
     * </p>
     */
    @Scheduled(cron = "0 0 9 * * ?")
    public void pushNewSourcesDetail() {
        String traceId = IdUtil.fastSimpleUUID();
        StopWatch sw = new StopWatch(traceId);
        log.info(">>>> [Task_SourceDetail_Start] TraceID: {}, 开始执行信源检测...", traceId);

        try {
            // 阶段 1: 初始化基准ID (如果应用刚重启)
            if (currentBaseId == null) {
                sw.start("Init_BaseID");
                // 查出当前最大的ID，作为后续比对的基准
                String maxIdSql = "SELECT MAX(id) FROM trs_datasource";
                Long maxId = jdbcTemplate.queryForObject(maxIdSql, Long.class);
                // 如果库里没数据，默认从0开始
                currentBaseId = (maxId != null) ? maxId : 0L;
                log.info(">>>> [Task_SourceDetail_Init] TraceID: {}, 系统首次运行或重启，基准ID初始化为: {}", traceId, currentBaseId);
                sw.stop();
                return; // 首次初始化不发送邮件，仅建立基准
            }

            // 阶段 2: 查询增量数据
            sw.start("Query_Delta_Data");
            log.info(">>>> [Task_SourceDetail_Query] TraceID: {}, 当前基准ID: {}", traceId, currentBaseId);
            // 查询 ID 比基准值大，且创建时间在一天内的数据（双重保障）
            String sql = "SELECT * FROM trs_datasource WHERE id > ? AND createTime >= DATE_SUB(CURDATE(), INTERVAL 1 DAY) ORDER BY id DESC";
            List<Map<String, Object>> newSources = jdbcTemplate.queryForList(sql, currentBaseId);
            sw.stop();

            // 阶段 3: 处理增量数据
            if (CollUtil.isNotEmpty(newSources)) {
                sw.start("Send_Email");
                log.info(">>>> [Task_SourceDetail_Process] TraceID: {}, 检出增量数据 {} 条", traceId, newSources.size());

                // 发送邮件
                buildAndSendSourceEmail(newSources);

                // 更新基准ID为当前批次中最大的ID
                Long newMaxId = Long.parseLong(newSources.get(0).get("id").toString());
                log.info(">>>> [Task_SourceDetail_Update] TraceID: {}, 基准更新: {} -> {}", traceId, currentBaseId, newMaxId);
                currentBaseId = newMaxId;
                sw.stop();
            } else {
                log.info(">>>> [Task_SourceDetail_Idle] TraceID: {}, 无新增信源", traceId);
            }
        } catch (Exception e) {
            log.error(">>>> [Task_SourceDetail_Error] TraceID: {}, 异常细节: ", traceId, e);
        } finally {
            if (sw.isRunning()) {
                sw.stop();
            }
            log.info(">>>> [Task_SourceDetail_End] TraceID: {}, 总执行耗时: {}ms", traceId, sw.getTotalTimeMillis());
        }
    }

    /**
     * 2. 多端口连接探测
     * 对应需求：9092, 9100, 10086
     */
    @Scheduled(cron = "0 0/10 * * * ?")
    public void monitorServerPorts() {
        log.info(">>>> [Task_PortMonitor_Start] 开始端口连通性扫描...");

        if (dailyPortAlarmCount >= PORT_ALARM_DAILY_LIMIT) {
            log.warn(">>>> [Task_PortMonitor_Limit] 今日端口告警已达上限 ({}), 停止探测", PORT_ALARM_DAILY_LIMIT);
            return;
        }

        String[] ports = serverPorts.split(",");
        StringBuilder errorMsg = new StringBuilder();
        boolean hasError = false;

        for (String portStr : ports) {
            int port = Integer.parseInt(portStr.trim());
            try (Socket socket = new Socket()) {
                // 尝试建立连接
                socket.connect(new InetSocketAddress(serverIp, port), SOCKET_TIMEOUT_MS);
                log.debug(">>>> [Port_Check] {}:{} -> CONNECTED", serverIp, port);
            } catch (IOException e) {
                hasError = true;
                errorMsg.append(port).append(" ");
                log.warn(">>>> [Port_Check] {}:{} -> FAILED, Reason: {}", serverIp, port, e.getMessage());
            }
        }

        if (hasError) {
            log.error(">>>> [Port_Alarm] 触发告警! 故障端口列表: {}", errorMsg);
            sendEmailAlarm("服务器端口连通性异常告警", "服务器 " + serverIp + " 探测失败端口: " + errorMsg + "<br/>请检查 9100(TRS) 和 10086(消费) 服务状态。");
            dailyPortAlarmCount++;
        }
        log.info(">>>> [Task_PortMonitor_End] 探测周期结束");
    }

    /**
     * 3. 数据流断流监测
     */
    @Scheduled(cron = "0 0/30 * * * ?")
    public void monitorDataFlow() {
        log.info(">>>> [Task_DataFlow_Start] 开始数据流活性检测...");
        try {
            String sql = "SELECT COUNT(*) FROM article WHERE createTime >= DATE_SUB(NOW(), INTERVAL 1 HOUR)";
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class);

            if (count != null && count > 0) {
                lastSeenDataTime = LocalDateTime.now();
                log.info(">>>> [Task_DataFlow_Active] 状态正常. 过去1小时入库数据量: {}", count);
            } else {
                long hours = Duration.between(lastSeenDataTime, LocalDateTime.now()).toHours();
                log.warn(">>>> [Task_DataFlow_Inactive] 活性预警：已连续 {} 小时未发现新数据", hours);
                if (hours >= DATA_STOP_THRESHOLD_HOURS) {
                    log.error(">>>> [Task_DataFlow_Alarm] 触发断流告警! 持续时长: {}h", hours);
                    sendEmailAlarm("数据流断流警告", "服务器 " + serverIp + " 已连续 " + hours + " 小时无新数据入库。");
                }
            }
        } catch (Exception e) {
            log.error(">>>> [Task_DataFlow_Error] SQL执行异常: ", e);
        }
    }

    /**
     * 4. 阶梯告警逻辑
     */
    @Scheduled(cron = "0 0/30 * * * ?")
    public void monitorFailureRate() {
        log.info(">>>> [Task_FailureRate_Start] 开始计算今日入库失败率阶梯...");
        try {
            // 排除掉 "链接过长" 这种非系统错误的业务异常
            String sql = "SELECT COUNT(*) FROM article WHERE createTime >= CURDATE() AND isVideoTranscod = 3 AND resourceUrl != ?";
            Integer failCount = jdbcTemplate.queryForObject(sql, Integer.class, ERR_MSG_LINK_TOO_LONG);

            if (failCount != null) {
                int level = 0;
                if (failCount >= FAIL_LEVEL_L3) level = 3;
                else if (failCount >= FAIL_LEVEL_L2) level = 2;
                else if (failCount >= FAIL_LEVEL_L1) level = 1;

                log.info(">>>> [Task_FailureRate_Stats] 今日累计失败数: {}, 当前匹配告警级别: L{}", failCount, level);

                // 只有当故障等级升级时才报警（例如从 L1 变成 L2）
                if (level > lastReportedLevel) {
                    log.warn(">>>> [Task_FailureRate_Alarm] 告警升级! L{} -> L{}", lastReportedLevel, level);
                    sendEmailAlarm("入库失败阶梯告警(L" + level + ")", "今日累计失败: " + failCount);
                    lastReportedLevel = level;
                }
            }
        } catch (Exception e) {
            log.error(">>>> [Task_FailureRate_Error] 监控统计失败: ", e);
        }
    }

    /**
     * 5. 零点状态重置
     * 每天0点执行，重置计数器
     */
    @Scheduled(cron = "0 0 0 * * ?")
    public void resetDailyCounters() {
        log.info(">>>> [Maintenance_Reset] 触发零点例行维护. 重置前状态: portAlarms={}, maxLevel={}",
                dailyPortAlarmCount, lastReportedLevel);
        dailyPortAlarmCount = 0;
        lastReportedLevel = 0;
        log.info(">>>> [Maintenance_Reset] 重置完成，新的一天开始。");
    }

    /**
     * 辅助方法：构建并发送包含信源详情和端口状态的邮件
     */
    private void buildAndSendSourceEmail(List<Map<String, Object>> data) {
        log.info(">>>> [Report_Gen] 开始构建 HTML 体检报告, 数据项: {}条", data.size());
        StringBuilder body = new StringBuilder();

        body.append("<h2>核心服务端口体检报告</h2>");
        body.append("<table border='1' cellspacing='0' cellpadding='5' style='border-collapse: collapse;'>");
        body.append("<tr style='background-color: #f2f2f2;'><th>服务名称</th><th>端口</th><th>状态</th></tr>");

        // 重点关注的两个服务
        body.append(formatPortStatusRow("TRS数据接收服务", 9100));
        body.append(formatPortStatusRow("文章消费服务", 10086));
        body.append("</table><br/><hr/><br/>");

        body.append("<h2>昨日新增信源明细</h2>");
        body.append("<table border='1' cellspacing='0' cellpadding='5' style='border-collapse: collapse;'>");
        body.append("<tr style='background-color: #f2f2f2;'><th>ID</th><th>信源名称</th></tr>");
        for (Map<String, Object> row : data) {
            body.append("<tr><td>").append(row.get("id")).append("</td><td>").append(row.get("source_name")).append("</td></tr>");
        }
        body.append("</table>");

        sendEmailAlarm("每日文章消费综合体检报告", body.toString());
    }

    /**
     * 辅助方法：检测单个端口并返回HTML行
     */
    private String formatPortStatusRow(String serviceName, int port) {
        String status;
        String color;

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(serverIp, port), SOCKET_TIMEOUT_MS);
            status = "正常 (Running)";
            color = "green";
            log.debug(">>>> [Report_Port_Check] {}:{} -> OK", serverIp, port);
        } catch (IOException e) {
            status = "异常 (Connection Failed)";
            color = "red";
            log.error(">>>> [Report_Port_Check] {}:{} -> ERROR: {}", serverIp, port, e.getMessage());
        }

        return String.format("<tr><td>%s</td><td>%d</td><td style='color: %s; font-weight: bold;'>%s</td></tr>",
                serviceName, port, color, status);
    }

    /**
     * 辅助方法：发送邮件封装
     */
    private void sendEmailAlarm(String title, String content) {
        log.info(">>>> [Email_Action] 准备发送邮件: [{}]", title);
        try {
            MailUtil.send(targetEmail, title, content, true);
            log.info(">>>> [Email_Success] 邮件发送成功, 接收人: {}", targetEmail);
        } catch (Exception e) {
            log.error(">>>> [Email_Error] 邮件发送失败! 标题: {}, 异常类型: {}, 错误消息: {}",
                    title, e.getClass().getSimpleName(), e.getMessage());
        }
    }
}