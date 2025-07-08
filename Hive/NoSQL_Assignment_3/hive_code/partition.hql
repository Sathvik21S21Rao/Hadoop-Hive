CREATE TABLE data_warehouse_pb (
    student_id STRING,
    student_name STRING,
    program STRING,
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
PARTITIONED BY (batch STRING)
CLUSTERED BY (course) INTO 4 BUCKETS
STORED AS ORC;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;



INSERT TABLE data_warehouse_pb PARTITION (batch)
SELECT 
    student_id,
    student_name,
    program,
    course,
    faculty,
    course_type,
    period,
    enrollment_date,
    classes_attended,
    classes_absent,
    avg_attendance_percent,
    obtained_grade,
    total_marks,
    exam_result,
    batch
FROM data_warehouse;
