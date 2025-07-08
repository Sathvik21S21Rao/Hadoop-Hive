CREATE TABLE IF NOT EXISTS data_warehouse (
    student_id STRING,
    student_name STRING,
    program STRING,
    batch STRING,
    course STRING,
    faculty ARRAY<STRING>,
    course_type STRING,
    period STRING,
    enrollment_date DATE,
    classes_attended INT,
    classes_absent INT,
    avg_attendance_percent DECIMAL(5,2),
    obtained_grade STRING,
    total_marks STRING,
    exam_result STRING
)
STORED AS ORC;

WITH valid_attendance AS (
    SELECT * FROM course_attendance a
    WHERE NOT EXISTS (
        SELECT 1 FROM error_records e
        WHERE e.source_table = 'course_attendance'
          AND e.original_row LIKE CONCAT('%', a.member_id_hash, '%')
    )
),
valid_enrollment AS (
    SELECT * FROM enrollment_data e
    WHERE NOT EXISTS (
        SELECT 1 FROM error_records er
        WHERE er.source_table = 'enrollment_data'
          AND er.original_row LIKE CONCAT('%', e.student_id, '%')
    )
),
valid_grades AS (
    SELECT * FROM grade_roster_distinct g
    WHERE NOT EXISTS (
        SELECT 1 FROM error_records er
        WHERE er.source_table = 'grade_roster_report'
          AND er.original_row LIKE CONCAT('%', g.student_id, '%')
    )
)

INSERT INTO TABLE data_warehouse
SELECT
    e.student_id,
    e.student_name,
    e.program,
    e.batch,
    e.course,
    e.primary_faculty AS faculty,
    e.course_type,
    e.period,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(e.enrollment_date, 'dd/MM/yy'))) AS enrollment_date,
    a.classes_attended,
    a.classes_absent,
    a.avg_attendance_percent_1,
    g.obtained_marks_grade,
    g.out_of_marks_grade,
    g.exam_result
FROM valid_enrollment e
JOIN valid_attendance a ON e.course = a.course AND e.student_id = a.member_id_hash
JOIN valid_grades g ON e.student_id = g.student_id AND g.subject_code_name = e.course;

