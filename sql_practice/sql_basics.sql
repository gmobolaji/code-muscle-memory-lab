-- 1. Conditional Logic
SELECT 
    employee_id,
    salary,
    CASE 
        WHEN salary < 50000 THEN 'Entry Level'
        WHEN salary BETWEEN 50000 AND 100000 THEN 'Mid Level'
        ELSE 'Senior Level'
    END AS role_level
FROM employees;

-- 2. Aggregate and Grouping
SELECT department_id, 
       COUNT(*) AS headcount, 
       AVG(salary) AS avg_salary
FROM employees
GROUP BY department_id;

-- 3. Joins
SELECT e.employee_id, e.name, d.department_name
FROM employees e
JOIN departments d 
    ON e.department_id = d.department_id;
