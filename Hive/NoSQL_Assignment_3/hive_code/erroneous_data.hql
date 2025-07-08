CREATE TABLE IF NOT EXISTS error_records (
    source_table STRING,
    original_row STRING,
    column_name STRING,
    issue_type STRING
)
STORED AS TEXTFILE;

INSERT INTO error_records
SELECT 'course_attendance',
       CONCAT_WS(',', course, CONCAT_WS(';', instructors), name_hash, email_hash, member_id_hash,
                      CAST(classes_attended AS STRING), CAST(classes_absent AS STRING), 
                      CAST(avg_attendance_percent AS STRING)),
       column_name,
       issue_type
FROM (
    SELECT *,
           'course' AS column_name,
           'Missing Value' AS issue_type
    FROM course_attendance
    WHERE course IS NULL OR TRIM(course) = ''

    UNION ALL

    SELECT *,
           'instructors' AS column_name,
           'Missing Value' AS issue_type
    FROM course_attendance
    WHERE instructors IS NULL OR size(instructors) = 0 
          OR array_contains(instructors, '')

    UNION ALL

    SELECT *,
           'email_hash' AS column_name,
           'Missing Value' AS issue_type
    FROM course_attendance
    WHERE email_hash IS NULL OR TRIM(email_hash) = ''

    UNION ALL

    SELECT *,
           'email_hash' AS column_name,
           'Invalid Format' AS issue_type
    FROM course_attendance
    WHERE email_hash IS NOT NULL 
          AND NOT email_hash RLIKE '^[a-fA-F0-9]{64}$'

    UNION ALL

    SELECT *,
           'member_id_hash' AS column_name,
           'Missing Value' AS issue_type
    FROM course_attendance
    WHERE member_id_hash IS NULL OR TRIM(member_id_hash) = ''

    UNION ALL

    SELECT *,
           'avg_attendance_percent' AS column_name,
           'Out of Range' AS issue_type
    FROM course_attendance
    WHERE avg_attendance_percent IS NOT NULL 
          AND (avg_attendance_percent < 0 OR avg_attendance_percent > 100)
) AS t;

INSERT INTO error_records
SELECT 'enrollment_data',
       CONCAT_WS(',', 
           CAST(serial_no AS STRING),
           course,
           status,
           course_type,
           course_variant,
           academia_lms,
           student_id,
           student_name,
           program,
           batch,
           period,
           enrollment_date,
           CONCAT_WS(';', primary_faculty)
       ),
       column_name,
       issue_type
FROM (
    -- 1. Missing enrollment_date
    SELECT *, 'enrollment_date' AS column_name, 'Missing Value' AS issue_type
    FROM enrollment_data
    WHERE enrollment_date IS NULL OR TRIM(enrollment_date) = ''

    UNION ALL

    -- 2. Invalid date format (not dd/MM/yy)
    SELECT *, 'enrollment_date' AS column_name, 'Invalid Format' AS issue_type
    FROM enrollment_data
    WHERE enrollment_date IS NOT NULL
) AS t;

INSERT INTO error_records
SELECT 'grade_roster_report',
       CONCAT_WS(',', 
           academy_location,
           student_id,
           student_status,
           admission_id,
           admission_status,
           student_name,
           program_code_name,
           batch,
           period,
           subject_code_name,
           course_type,
           section,
           faculty_name,
           CAST(course_credit AS STRING),
           obtained_marks_grade,
           out_of_marks_grade,
           exam_result
       ),
       column_name,
       issue_type
FROM (
    -- 1. Missing exam_result
    SELECT *, 'exam_result' AS column_name, 'Missing Value' AS issue_type
    FROM grade_roster_report
    WHERE exam_result IS NULL OR TRIM(exam_result) = ''

    UNION ALL

    -- 2. Invalid exam_result (must be PASS or FAIL)
    SELECT *, 'exam_result' AS column_name, 'Invalid Format' AS issue_type
    FROM grade_roster_report
    WHERE exam_result IS NOT NULL 
      AND UPPER(TRIM(exam_result)) NOT IN ('PASS', 'FAIL')

    UNION ALL

    -- 3. Invalid obtained_marks_grade
    SELECT *, 'obtained_marks_grade' AS column_name, 'Invalid Format' AS issue_type
    FROM grade_roster_report
    WHERE obtained_marks_grade IS NOT NULL
      AND UPPER(TRIM(obtained_marks_grade)) NOT IN (
          'A', 'A-', 'B+', 'B', 'B-', 'C+', 'C', 'D', 'F', 'S'
      )
) AS t;
