package com.familybudget.analytic;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import jakarta.annotation.PostConstruct;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;
import java.util.stream.Collectors;

@SpringBootApplication
@EnableScheduling
@EnableRabbit
public class AnalyticServiceApplication {

    private final JdbcTemplate jdbcTemplate;
    private final MeterRegistry meterRegistry;
    private final MultiGauge expensesByCategory;

    public static void main(String[] args) {
        SpringApplication.run(AnalyticServiceApplication.class, args);
    }

    public AnalyticServiceApplication(JdbcTemplate jdbcTemplate, MeterRegistry meterRegistry) {
        this.jdbcTemplate = jdbcTemplate;
        this.meterRegistry = meterRegistry;


        this.expensesByCategory = MultiGauge.builder("family_expenses_by_category")
                .description("Витрати по категоріях")
                .baseUnit("UAH")
                .register(meterRegistry);
    }


    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @RabbitListener(queuesToDeclare = @org.springframework.amqp.rabbit.annotation.Queue(name = "analytics_queue"))
    public void listenForExpenses(ExpenseMessage message) {
        System.out.println(">>> ОТРИМАНО ЧЕРЕЗ RABBITMQ: " + message.description() + " на суму " + message.amount());


        updateCategoryMetrics();
    }

    @PostConstruct
    public void initMetrics() {

        Gauge.builder("family_budget_total_expenses", this, value -> getTotalExpenses())
                .description("Загальна сума витрат")
                .register(meterRegistry);


        Gauge.builder("family_budget_last_salary", this, value -> getLastSalary())
                .description("Остання зарплата")
                .register(meterRegistry);
    }


    @Scheduled(fixedRate = 10000)
    public void updateCategoryMetrics() {
        String sql = """
            SELECT c.name AS category, SUM(t.amount) AS total
            FROM transactions t
            JOIN category c ON t.category_id = c.id
            GROUP BY c.name
        """;

        try {
            List<CategoryStat> stats = jdbcTemplate.query(sql, (rs, rowNum) ->
                    new CategoryStat(rs.getString("category"), rs.getDouble("total"))
            );

            List<MultiGauge.Row<?>> rows = stats.stream()
                    .map(stat -> MultiGauge.Row.of(Tags.of("category", stat.name), stat.total))
                    .collect(Collectors.toList());


            expensesByCategory.register(rows, true);

        } catch (Exception e) {
            System.err.println("Помилка метрик: " + e.getMessage());
        }
    }

    private double getTotalExpenses() {
        try {
            String sql = "SELECT SUM(amount) FROM transactions";
            Double result = jdbcTemplate.queryForObject(sql, Double.class);
            return result != null ? result : 0.0;
        } catch (Exception e) { return 0.0; }
    }

    private double getLastSalary() {
        try {
            String sql = "SELECT amount FROM salary ORDER BY received_at DESC LIMIT 1";
            Double result = jdbcTemplate.queryForObject(sql, Double.class);
            return result != null ? result : 0.0;
        } catch (Exception e) { return 0.0; }
    }


    public record ExpenseMessage(String description, Double amount, String category) {}

    private record CategoryStat(String name, Double total) {}
}