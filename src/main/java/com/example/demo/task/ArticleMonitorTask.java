package com.example.demo.task;

import cn.hutool.core.collection.CollUtil;
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
import java.util.List;
import java.util.Map;

/**
 * 文章消费监控核心任务
 * 遵循阿里巴巴 Java 开发手册规范
 *
 * @author admin
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

    // --- 静态常量 (Static Constants) ---

    /** SQL查询中：标识资源链接长度超限的错误消息文本 */
    private static final String ERR_MSG_LINK_TOO_LONG = "资源链接超过字段长度：8255";

    /** 数据断流告警阈值：连续 N 小时无数据则触发告警 */
    private static final int DATA_STOP_THRESHOLD_HOURS = 8;

    /** 每日端口异常邮件发送次数上限，防止邮件轰炸 */
    private static final int PORT_ALARM_DAILY_LIMIT = 2;

    /** 网络探测超时时间（单位：毫秒） */
    private static final int SOCKET_TIMEOUT_MS = 3000;

    /** 失败告警阶梯：第一级（一般严重） */
    private static final int FAIL_LEVEL_L1 = 20;

    /** 失败告警阶梯：第二级（高度严重） */
    private static final int FAIL_LEVEL_L2 = 50;

    /** 失败告警阶梯：第三级（灾难级别） */
    private static final int FAIL_LEVEL_L3 = 100;

    // --- 内部变量 (Instance/Static Variables) ---

    /** 最后一次从数据库中检测到新记录的时间 */
    private LocalDateTime lastSeenDataTime;

    /** 记录当日已发送的端口告警邮件次数 */
    private int dailyPortAlarmCount = 0;

    /** 记录当日已触发的最高告警级别，避免重复发送同级别的低阶告警 */
    private int lastReportedLevel = 0;

    /** * 增量监控的基准 ID。
     * 使用 volatile 关键字确保在多线程定时任务中的可见性。
     * 初始为 null，由任务逻辑自动完成初始化。
     */
    private volatile Long currentBaseId = null;

    @PostConstruct
    public void init() {
        this.lastSeenDataTime = LocalDateTime.now();
        log.info(">>>> [服务启动] 监控任务已就绪，当前目标服务器: {}", serverIp);
    }

    /**
     * 需求 5 优化：增量信源明细推送
     * 采用“懒加载”初始化 ID，若 currentBaseId 为空则先查询库内最大值
     */
    @Scheduled(cron = "0 0 9 * * ?")
    public void pushNewSourcesDetail() {
        try {
            // 1. 初始化检查：如果从未运行过，先获取库内当前最大ID作为起点
            if (currentBaseId == null) {
                String maxIdSql = "SELECT MAX(id) FROM trs_datasource";
                Long maxId = jdbcTemplate.queryForObject(maxIdSql, Long.class);
                currentBaseId = (maxId != null) ? maxId : 0L;
                log.info(">>>> [基准初始化] 获取当前数据库最大 ID 为: {}，从下一次循环开始监控增量。", currentBaseId);
                return;
            }

            // 2. 增量查询：查询 ID 大于基准且为昨日创建的数据
            String sql = "SELECT * FROM trs_datasource WHERE id > ? AND createTime >= DATE_SUB(CURDATE(), INTERVAL 1 DAY) ORDER BY id DESC";
            List<Map<String, Object>> newSources = jdbcTemplate.queryForList(sql, currentBaseId);

            // 无论是否有新数据，9点钟都可以通过 buildAndSendSourceEmail 发送“体检报告”
            // 如果你希望只有在有新数据时才发，可以保留这个 if 判断
            if (CollUtil.isNotEmpty(newSources)) {
                // 3. 构造邮件并发送
                buildAndSendSourceEmail(newSources);

                // 4. 更新基准 ID 为本次探测到的最新（最大）ID
                currentBaseId = Long.parseLong(newSources.get(0).get("id").toString());
                log.info(">>>> [增量监控] 发现 {} 条新信源，基准 ID 更新至: {}", newSources.size(), currentBaseId);
            } else {
                log.info(">>>> [常规巡检] 昨日无新增信源 (BaseID: {})", currentBaseId);
            }
        } catch (Exception e) {
            log.error(">>>> [任务异常] 信源推送任务执行失败", e);
        }
    }

    /**
     * 需求 6 优化：多端口连接探测
     * 监控 Kafka(9092)、数据接收(9100)、消费服务(10086)
     */
    @Scheduled(cron = "0 0/10 * * * ?")
    public void monitorServerPorts() {
        if (dailyPortAlarmCount >= PORT_ALARM_DAILY_LIMIT) return;

        String[] ports = serverPorts.split(",");
        StringBuilder errorMsg = new StringBuilder();
        boolean hasError = false;

        for (String portStr : ports) {
            int port = Integer.parseInt(portStr.trim());
            // 使用 try-with-resources 自动管理资源
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(serverIp, port), SOCKET_TIMEOUT_MS);
            } catch (IOException e) {
                hasError = true;
                errorMsg.append("端口 ").append(port).append(" 连通失败; ");
            }
        }

        if (hasError) {
            log.error(">>>> [端口报警] 发现异常端口：{}", errorMsg);
            sendEmailAlarm("服务器端口连通性异常告警", "服务器 " + serverIp + " 探测结果: " + errorMsg);
            dailyPortAlarmCount++;
        }
    }

    /**
     * 数据流断流监测
     */
    @Scheduled(cron = "0 0/30 * * * ?")
    public void monitorDataFlow() {
        try {
            String sql = "SELECT COUNT(*) FROM article WHERE createTime >= DATE_SUB(NOW(), INTERVAL 1 HOUR)";
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class);

            if (count != null && count > 0) {
                lastSeenDataTime = LocalDateTime.now();
            } else {
                long hours = Duration.between(lastSeenDataTime, LocalDateTime.now()).toHours();
                if (hours >= DATA_STOP_THRESHOLD_HOURS) {
                    sendEmailAlarm("数据流断流警告", "服务器 " + serverIp + " 已连续 " + hours + " 小时无新数据入库。");
                }
            }
        } catch (Exception e) {
            log.error(">>>> [异常] 数据流监控失败", e);
        }
    }

    /**
     * 阶梯告警逻辑
     */
    @Scheduled(cron = "0 0/30 * * * ?")
    public void monitorFailureRate() {
        try {
            String sql = "SELECT COUNT(*) FROM article WHERE createTime >= CURDATE() AND isVideoTranscod = 3 AND resourceUrl != ?";
            Integer failCount = jdbcTemplate.queryForObject(sql, Integer.class, ERR_MSG_LINK_TOO_LONG);

            if (failCount != null) {
                int level = 0;
                if (failCount >= FAIL_LEVEL_L3) level = 3;
                else if (failCount >= FAIL_LEVEL_L2) level = 2;
                else if (failCount >= FAIL_LEVEL_L1) level = 1;

                if (level > lastReportedLevel) {
                    sendEmailAlarm("入库失败阶梯告警(L" + level + ")", "今日累计失败: " + failCount);
                    lastReportedLevel = level;
                }
            }
        } catch (Exception e) {
            log.error(">>>> [异常] 失败率监控失败", e);
        }
    }

    @Scheduled(cron = "0 0 0 * * ?")
    public void resetDailyCounters() {
        dailyPortAlarmCount = 0;
        lastReportedLevel = 0;
        log.info(">>>> [系统维护] 每日告警计数器已重置。");
    }

    /**
     * 构建综合体检报告邮件
     */
    private void buildAndSendSourceEmail(List<Map<String, Object>> data) {
        StringBuilder body = new StringBuilder();

        // 1. 添加服务器健康状态板块
        body.append("<h2>核心服务端口体检报告</h2>");
        body.append("<table border='1' cellspacing='0' cellpadding='5' style='border-collapse: collapse;'>");
        body.append("<tr style='background-color: #f2f2f2;'><th>服务名称</th><th>端口</th><th>状态</th></tr>");

        // 检查 9100 和 10086 两个核心端口
        body.append(formatPortStatusRow("TRS数据接收服务", 9100));
        body.append(formatPortStatusRow("文章消费服务", 10086));
        body.append("</table>");

        body.append("<br/><hr/><br/>");

        // 2. 添加新增信源明细板块
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
     * 检测单个端口状态并格式化为 HTML 行
     */
    private String formatPortStatusRow(String serviceName, int port) {
        String status;
        String color;

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(serverIp, port), SOCKET_TIMEOUT_MS);
            status = "正常 (Running)";
            color = "green";
        } catch (IOException e) {
            status = "异常 (Connection Failed)";
            color = "red";
        }

        return String.format("<tr><td>%s</td><td>%d</td><td style='color: %s; font-weight: bold;'>%s</td></tr>",
                serviceName, port, color, status);
    }

    private void sendEmailAlarm(String title, String content) {
        try {
            MailUtil.send(targetEmail, title, content, true);
        } catch (Exception e) {
            log.error(">>>> [邮件失败] 标题: {}", title);
        }
    }
}