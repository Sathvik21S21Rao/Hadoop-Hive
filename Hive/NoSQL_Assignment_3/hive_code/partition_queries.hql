WITH avg_attendance AS (
    SELECT
        course,
        batch,
        ROUND(AVG(CAST(REPLACE(avg_attendance_percent, '%', '') AS FLOAT)), 2) AS avg_attendance
    FROM data_warehouse_pb
    WHERE avg_attendance_percent IS NOT NULL
    GROUP BY course, batch
),
grade_counts AS (
    SELECT
        course,
        batch,
        obtained_grade,
        COUNT(*) AS grade_count
    FROM data_warehouse_pb
    WHERE obtained_grade IS NOT NULL
    GROUP BY course, batch, obtained_grade
),
mode_grade AS (
    SELECT
        course,
        batch,
        obtained_grade AS mode_grade
    FROM (
        SELECT
            course,
            batch,
            obtained_grade,
            grade_count,
            ROW_NUMBER() OVER (PARTITION BY course, batch ORDER BY grade_count DESC, obtained_grade ASC) AS rn
        FROM grade_counts
    ) ranked
    WHERE rn = 1
)
SELECT
    a.course,
    a.batch,
    a.avg_attendance,
    m.mode_grade
FROM avg_attendance a
JOIN mode_grade m
    ON a.course = m.course AND a.batch = m.batch
WHERE a.course NOT LIKE '%HPC%'
ORDER BY a.course, a.batch;


--query 2
SELECT
    course,
    batch,
    ROUND(
        100.0 * COUNT(CASE WHEN CAST(REPLACE(avg_attendance_percent, '%', '') AS FLOAT) < 50 THEN 1 END) 
        / COUNT(*), 
        2
    ) AS percentage_below_50_attendance
FROM data_warehouse_pb
WHERE avg_attendance_percent IS NOT NULL and course not like "%HPC"
GROUP BY course, batch
ORDER BY course, batch;
--query 3
SELECT *
FROM (
    SELECT
        course,
        batch,
        ROUND(AVG(CAST(REPLACE(avg_attendance_percent, '%', '') AS FLOAT)), 2) AS avg_attendance,
        ROW_NUMBER() OVER (PARTITION BY batch ORDER BY AVG(CAST(REPLACE(avg_attendance_percent, '%', '') AS FLOAT)) DESC) AS rank
    FROM data_warehouse_pb
    WHERE avg_attendance_percent IS NOT NULL and course not like "%HPC"
    GROUP BY course, batch
) ranked_courses
WHERE rank <= 3
ORDER BY batch, rank;