package com.redis.benchmark.utils;

import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.function.Consumer;
import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.ImageHtmlEmail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class EmailAlertUtils implements Consumer<String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmailAlertUtils.class);
    private static final String WHOAMI = "EmailAlert";

    public void sendHtmlEmail(String to, String subject, String text, String priority) {

        try {
            ImageHtmlEmail htmlEmail = new ImageHtmlEmail();

            htmlEmail.setHostName(BenchmarkConfiguration.get().getMailSmtpHost());
            htmlEmail.setSmtpPort(BenchmarkConfiguration.get().getMailSmtpPort());
            htmlEmail.setAuthenticator(new DefaultAuthenticator(BenchmarkConfiguration.get().getMailSmtpUserName(), BenchmarkConfiguration.get().getMailSmtpPassword()));

            htmlEmail.setStartTLSEnabled(BenchmarkConfiguration.get().isMailSmtpStartTLSEnabled());
            htmlEmail.setStartTLSRequired(BenchmarkConfiguration.get().isMailSmtpStartTLSRequired());
            htmlEmail.setFrom("no-reply@redis.com", "Jedis Alert");

            StringTokenizer st = new StringTokenizer(BenchmarkConfiguration.get().getMailTo(), ",");
            while (st.hasMoreTokens())
                htmlEmail.addTo(st.nextToken());

            htmlEmail.setSubject(subject);
            htmlEmail.setHtmlMsg(text); // Set the html message
            htmlEmail.setHeaders(new HashMap<>() {{
                put("X-Priority", priority);
            }});

            htmlEmail.setDebug(BenchmarkConfiguration.get().isMailDebug());

            htmlEmail.send(); // Send the email

            if (LOGGER.isDebugEnabled())
                LOGGER.debug("Sent an email alert to {}", to);

        } catch (Exception e) {
            LOGGER.error("Failed during {} {}", WHOAMI, e.getMessage());
        }
    }

    @Override
    public void accept(String clusterName) {
        LOGGER.warn("Jedis failed-over to cluster: " + clusterName);

        if (BenchmarkConfiguration.get().isMailAlertEnabled()) {
            String text = "<html>" +
                    "Hello,<br/><br/>" + "Jedis: " +
                    "Failed over to <b>" + clusterName + "</b> " +
                    "</html>";
            sendHtmlEmail(BenchmarkConfiguration.get().getMailTo(), clusterName, text, "1");
        }
    }
}
