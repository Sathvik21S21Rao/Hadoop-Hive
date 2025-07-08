
CREATE EXTERNAL TABLE IF NOT EXISTS course_attendance_staging (
    course STRING,
    instructors STRING,
    name_hash STRING,
    email_hash STRING,
    member_id_hash STRING,
    classes_attended INT,
    classes_absent INT,
    avg_attendance_percent STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        "separatorChar" = ",",
        "quoteChar" = "\"",
        "collection.items.delim" = ","  -- splits array field using comma
    )
    STORED AS TEXTFILE
    LOCATION '/user/hive/warehouse/univ_db_final.db/course_attendance'
    TBLPROPERTIES (
        "serialization.null.format" = ""
    );

CREATE TABLE IF NOT EXISTS course_attendance AS (
    SELECT course,name_hash,email_hash,member_id_hash,classes_attended,classes_absent,
    split(regexp_replace(instructors, '"', ''), ',') AS instructors, 
    CAST(REGEXP_REPLACE(avg_attendance_percent, '%', '') AS DECIMAL(5,2)) AS avg_attendance_percent1 FROM course_attendance_staging
);

CREATE EXTERNAL TABLE IF NOT EXISTS enrollment_data_staging (
        serial_no INT,
        course STRING,
        status STRING,
        course_type STRING,
        course_variant STRING,
        academia_lms STRING,
        student_id STRING,
        student_name STRING,
        program STRING,
        batch STRING,
        period STRING,
        enrollment_date STRING,
        primary_faculty STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        "separatorChar" = ",",
        "quoteChar" = "\"",
        "collection.items.delim" = ","  -- splits array field using comma
    )
    STORED AS TEXTFILE
    LOCATION '/user/hive/warehouse/univ_db_final.db/enrollment_data'
    TBLPROPERTIES (
        "serialization.null.format" = ""
);

CREATE TABLE enrollment_data AS
SELECT
  serial_no,
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
  split(regexp_replace(primary_faculty, '"', ''), ',') AS primary_faculty
FROM enrollment_data_staging;
 
CREATE TABLE IF NOT EXISTS grade_roster_report (
    academy_location STRING,
    student_id STRING,
    student_status STRING,
    admission_id STRING,
    admission_status STRING,
    student_name STRING,
    program_code_name STRING,
    batch STRING,
    period STRING,
    subject_code_name STRING,
    course_type STRING,
    section STRING,
    faculty_name STRING,
    course_credit INT,
    obtained_marks_grade STRING,
    out_of_marks_grade STRING,
    exam_result STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/hive/warehouse/univ_db_final.db/grade_roster_report'
TBLPROPERTIES (
    "serialization.null.format"=""
);

CREATE TABLE IF NOT EXISTS error_records (
    source_table STRING,
    original_row STRING,
    column_name STRING,
    issue_type STRING,     
    error_description STRING
)
STORED AS TEXTFILE;

DROP table course_attendance_staging;
DROP table enrollment_data_staging;


