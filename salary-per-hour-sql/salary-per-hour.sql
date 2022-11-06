CREATE OR REPLACE TABLE `playground.salary_per_hour` AS
    WITH transform_hours AS(
    SELECT e.employee_id
            ,e.branch_id
            ,e.salary
            ,t.date
            ,EXTRACT(HOUR FROM t.checkout-t.checkin)+
            (EXTRACT(MINUTE FROM t.checkout-t.checkin)/60)+
            (EXTRACT(SECOND FROM t.checkout-t.checkin)/3600) AS hours_raw
            ,AVG(EXTRACT(HOUR FROM t.checkout-t.checkin)+
            (EXTRACT(MINUTE FROM t.checkout-t.checkin)/60)+
            (EXTRACT(SECOND FROM t.checkout-t.checkin)/3600)) OVER(PARTITION BY e.employee_id, e.branch_id ORDER BY date ASC
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING ) AS mov_avg_hours
            ,AVG(EXTRACT(HOUR FROM t.checkout-t.checkin)+
            (EXTRACT(MINUTE FROM t.checkout-t.checkin)/60)+
            (EXTRACT(SECOND FROM t.checkout-t.checkin)/3600)) OVER(PARTITION BY e.branch_id) AS avg_branch_hours
    FROM `playground.employees` e
    INNER JOIN `playground.timesheets` t
    ON e.employee_id = t.employee_id
    )
    ,missing_hours_handling AS(
    SELECT employee_id
            ,branch_id
            ,salary
            ,date
            ,IFNULL(CASE WHEN hours_raw IS NULL THEN mov_avg_hours
                        ELSE hours_raw END 
                    ,avg_branch_hours) AS hours
    FROM transform_hours
    )
    ,get_distinct_salary AS(
    SELECT DISTINCT EXTRACT(YEAR FROM date) AS year
        ,EXTRACT(MONTH FROM date) AS month
        ,employee_id
        ,branch_id
        ,salary
    FROM missing_hours_handling
    )
    ,get_total_salaries AS(
    SELECT year
        ,month
        ,branch_id
        ,COUNT(employee_id) as total_employees
        ,SUM(salary) AS total_salaries
    FROM get_distinct_salary
    GROUP BY 1, 2, 3
    )
    ,get_total_hours AS(
    SELECT EXTRACT(YEAR FROM date) AS year
        ,EXTRACT(MONTH FROM date) AS month
        ,branch_id
        ,SUM(hours) total_hours
    FROM missing_hours_handling
    GROUP BY 1, 2, 3
    )
    SELECT a.year
        ,a.month
        ,a.branch_id
        ,a.total_employees
        ,a.total_salaries
        ,b.total_hours
        ,a.total_salaries/b.total_hours as salary_per_hour
    FROM get_total_salaries a
    INNER JOIN get_total_hours b
        ON a.year = b.year 
        AND a.month = b.month
        AND a.branch_id = b.branch_id
    ORDER BY 1, 2, CAST(branch_id AS INT64) ASC