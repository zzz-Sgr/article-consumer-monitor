package com.example.demo.task;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.RuntimeUtil;
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
 * @description: 综合监控任务：生产环境最终优化版
 * @author: admin
 * @date: 2026-02-11
 */

@Component
@Slf4j
public class ArticleMonitorTask {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${monitor.alarm.email-to}")
    private String targetEmail;

    @Value("${monitor.server.ip}")
    private String serverIp;

    @Value("${monitor.server.port}")
    private int serverPort;

    @Value("${monitor.kafka.script-path}")
    private String kafkaScriptPath;

    // [优化 2] 提取基准 ID 到配置文件
    @Value("${monitor.source.base-id}")
    private int baseSourceId;

    // --- 常量定义 ---
    private static final String ERR_MSG_LINK_TOO_LONG = "资源链接超过字段长度：8255";
    private static final int DATA_STOP_THRESHOLD_HOURS = 8;
    private static final int PORT_ALARM_DAILY_LIMIT = 2;
    // private static final int BASE_SOURCE_ID = 334; // 已移除硬编码

    private static final int FAIL_LEVEL_L1 = 20;
    private static final int FAIL_LEVEL_L2 = 50;
    private static final int FAIL_LEVEL_L3 = 100;

    // --- 状态变量 ---
    private LocalDateTime lastSeenDataTime;
    private int dailyPortAlarmCount = 0;
    private int lastReportedLevel = 0;

    /**
     * 初始化最后入库时间
     */
    @PostConstruct
    public void initLastSeenTime() {
        try {
            String sql = "SELECT MAX(createTime) FROM article";
            LocalDateTime lastTime = jdbcTemplate.queryForObject(sql, LocalDateTime.class);
            this.lastSeenDataTime = (lastTime != null) ? lastTime : LocalDateTime.now();
            log.info(">>>> [系统初始化] 成功获取最后入库时间: {}, 服务器: {}", lastSeenDataTime, serverIp);
        } catch (Exception e) {
            this.lastSeenDataTime = LocalDateTime.now();
            log.warn(">>>> [系统初始化] 无法获取最后入库时间。");
        }
    }

    /**
     * 需求 4: 数据流断流监测
     * [优化 3] 提高检测频率：改为每 30 分钟执行一次，SQL 依然查过去 1 小时。
     * 这样可以更敏锐地发现断流（最快断流 30 分钟后就能发现，而不是 1 小时）。
     */
    @Scheduled(cron = "0 0/30 * * * ?") // 修改为：每30分钟执行
    public void monitorDataFlow() {
        try {
            String sql = "SELECT COUNT(*) FROM article WHERE createTime >= DATE_SUB(NOW(), INTERVAL 1 HOUR)";
            Integer recordCount = jdbcTemplate.queryForObject(sql, Integer.class);

            if (recordCount != null && recordCount > 0) {
                lastSeenDataTime = LocalDateTime.now();
                log.info(">>>> [正常] 数据流正常。服务器: {}, 过去1小时入库数: {}", serverIp, recordCount);
            } else {
                long hoursOffline = Duration.between(lastSeenDataTime, LocalDateTime.now()).toHours();
                if (hoursOffline >= DATA_STOP_THRESHOLD_HOURS) {
                    sendEmailAlarm("数据流断流严重告警", "服务器 " + serverIp + " 的 article 表已连续 " + hoursOffline + " 小时无新数据。");
                } else {
                    log.warn(">>>> [预警阶段] 监测到数据流中断。当前累计断流: {} 小时", hoursOffline);
                }
            }
        } catch (Exception e) {
            log.error(">>>> [执行异常] 数据流监测方法失败。", e);
        }
    }

    /**
     * 需求 5: 端口连接探测 (每 10 分钟)
     */
    @Scheduled(cron = "0 0/10 * * * ?") // 生产配置：每10分钟执行
    public void monitorServerPortConnection() {
        if (dailyPortAlarmCount >= PORT_ALARM_DAILY_LIMIT) return;

        boolean portOk = false;
        String os = System.getProperty("os.name").toLowerCase();

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(serverIp, serverPort), 3000);
            portOk = true;
            log.info(">>>> [监测正常] 端口连通性测试通过: {}:{}", serverIp, serverPort);
        } catch (IOException e) {
            log.warn(">>>> [监测异常] 端口无法连接: {}", e.getMessage());
        }

        // 只有 Linux 环境才深入检查进程
        if (portOk && os.contains("linux")) {
            String psResult = RuntimeUtil.execForStr("ps -ef | grep article | grep -v grep");
            String kafkaCmd = "sh " + kafkaScriptPath + " --bootstrap-server localhost:9092 --describe --group my-group";
            String kafkaResult = RuntimeUtil.execForStr(kafkaCmd);

            if (psResult.isEmpty() || kafkaResult.contains("error")) {
                portOk = false;
                log.error(">>>> [服务器异常] 进程或 Kafka 状态不符预期");
            }
        }

        if (!portOk) {
            sendEmailAlarm("监控状态异常", "服务器 " + serverIp + " 状态异常，请核查。");
            dailyPortAlarmCount++;
        }
    }

    /**
     * 需求 6: 每日新增信源明细推送 (每天 09:00)
     * [优化 1] 统计范围改为“昨天全天”，确保 00:00~23:59 的数据不遗漏。
     */
    @Scheduled(cron = "0 0 9 * * ?") // 生产配置：每天09:00执行
    public void pushNewSourcesDetail() {
        log.info(">>>> [常规任务] 正在推送昨日新增信源明细。服务器: {}", serverIp);
        try {
            // [优化 1] SQL 修改：查询昨天全天的数据 ( >= 昨天0点 AND < 今天0点 )
            String sql = "SELECT * FROM trs_datasource WHERE id > ? AND createTime >= DATE_SUB(CURDATE(), INTERVAL 1 DAY) AND createTime < CURDATE() ORDER BY id DESC";

            // [优化 2] 使用配置注入的 baseSourceId
            List<Map<String, Object>> newSources = jdbcTemplate.queryForList(sql, baseSourceId);

            if (CollUtil.isNotEmpty(newSources)) {
                StringBuilder htmlBody = new StringBuilder("<h3>昨日新增信源明细报告</h3>") // 标题修改为“昨日”
                        .append("<p>统计范围：昨日全天 (00:00 - 23:59)</p>")
                        .append("<table border='1' cellspacing='0' cellpadding='5'>")
                        .append("<tr bgcolor='#f2f2f2'><th>ID</th><th>信源名称</th><th>数据摘要</th></tr>");

                for (Map<String, Object> source : newSources) {
                    htmlBody.append("<tr>")
                            .append("<td>").append(source.get("id")).append("</td>")
                            .append("<td>").append(source.get("source_name")).append("</td>")
                            .append("<td>").append(source.toString()).append("</td>")
                            .append("</tr>");
                }
                htmlBody.append("</table>");
                sendEmailAlarm("昨日新信源推送报告", htmlBody.toString()); // 邮件标题也改为“昨日”
            } else {
                log.info(">>>> [任务成功] 昨日无新信源。");
            }
        } catch (Exception e) {
            log.error(">>>> [任务异常] 信源推送失败。", e);
        }
    }

    /**
     * 需求 7: 失败数量阶梯告警 (每 30 分钟)
     */
    @Scheduled(cron = "0 0/30 * * * ?") // 生产配置：每30分钟执行
    public void monitorFailureRate() {
        try {
            // 生产 SQL：必须查失败条件
            String sql = "SELECT COUNT(*) FROM article WHERE createTime >= CURDATE() AND isVideoTranscod = 3 AND resourceUrl != ?";
            Integer failCount = jdbcTemplate.queryForObject(sql, Integer.class, ERR_MSG_LINK_TOO_LONG);

            if (failCount != null) {
                int currentLevel = 0;
                if (failCount >= FAIL_LEVEL_L3) currentLevel = 3;
                else if (failCount >= FAIL_LEVEL_L2) currentLevel = 2;
                else if (failCount >= FAIL_LEVEL_L1) currentLevel = 1;

                if (currentLevel > lastReportedLevel) {
                    String title = "入库失败告警 (L" + currentLevel + ")";
                    String content = "服务器: " + serverIp + "\n今日累计入库失败数已达: " + failCount + " 次。";

                    if (currentLevel == 3) {
                        title = "系统严重报错 (L3)";
                        content += "\n请立即排查资源及服务状态！";
                    }

                    sendEmailAlarm(title, content);
                    lastReportedLevel = currentLevel;
                    log.info(">>>> [告警触发] 已发送 {} 级告警，当前失败数: {}", currentLevel, failCount);
                }
            }
        } catch (Exception e) {
            log.error(">>>> [统计异常] 失败率任务失败。", e);
        }
    }

    /**
     * 需求 8: 每日 08:50 定时发送健康检查报告
     */
    @Scheduled(cron = "0 50 8 * * ?") // 生产配置：每天08:50执行
    public void dailyHealthReport() {
        log.info(">>>> [常规任务] 开始生成每日健康检查报告。");

        String os = System.getProperty("os.name").toLowerCase();
        // 生产环境：非 Linux 直接跳过，避免报错
        if (!os.contains("linux")) {
            log.warn(">>>> [任务取消] 非 Linux 环境，跳过健康报告。");
            return;
        }

        String psResult = RuntimeUtil.execForStr("ps -ef | grep article | grep -v grep");
        if (psResult.isEmpty()) {
            psResult = "警告：未找到相关 article 进程！";
        }

        String kafkaCmd = "sh " + kafkaScriptPath + " --bootstrap-server localhost:9092 --describe --group my-group";
        String kafkaResult = RuntimeUtil.execForStr(kafkaCmd);
        if (kafkaResult.isEmpty() || kafkaResult.contains("error")) {
            kafkaResult = "错误：无法获取 Kafka 状态，请检查服务及脚本路径。";
        }

        StringBuilder htmlBody = new StringBuilder("<h2>每日系统健康巡检报告 (08:50)</h2>")
                .append("<p><b>服务器 IP:</b> ").append(serverIp).append("</p>")
                .append("<hr/>")
                .append("<h3>1. Java 进程状态 (ps -ef | grep article)</h3>")
                .append("<pre style='background:#f4f4f4;padding:10px;'>").append(psResult).append("</pre>")
                .append("<hr/>")
                .append("<h3>2. Kafka 消费组详情 (my-group)</h3>")
                .append("<pre style='background:#f4f4f4;padding:10px;'>").append(kafkaResult).append("</pre>")
                .append("<br/><p><i>注：此邮件为每日定时巡检。若运行期间进程异常中断，系统将触发实时告警。</i></p>");

        sendEmailAlarm("每日健康巡检报告", htmlBody.toString());
        log.info(">>>> [任务成功] 每日健康报告已发送至: {}", targetEmail);
    }

    /**
     * 每日重置计数器
     */
    @Scheduled(cron = "0 0 0 * * ?")
    public void resetDailyCounters() {
        dailyPortAlarmCount = 0;
        lastReportedLevel = 0;
        log.info(">>>> [系统维护] 每日计数器及阶梯等级已重置。");
    }

    private void sendEmailAlarm(String title, String content) {
        try {
            List<String> tos = cn.hutool.core.text.CharSequenceUtil.split(targetEmail, ',');
            if (CollUtil.isNotEmpty(tos)) {
                MailUtil.send(tos, title, content, true);
                log.info(">>>> [邮件发送成功] 标题: {}, 收件人数量: {}", title, tos.size());
            }
        } catch (Exception e) {
            log.error(">>>> [邮件发送失败] 请检查网络或 mail.setting 配置。", e);
        }
    }
}