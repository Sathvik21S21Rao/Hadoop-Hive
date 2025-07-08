raw = LOAD '/datawarehouse_export/000000_0' 
    USING PigStorage(',') 
    AS (
        student_id: chararray,
        student_name: chararray,
        program_name: chararray,
        batch: chararray,
        course: chararray,
        instructor: chararray,
        course_type: chararray,
        term: chararray,
        enrollment_date: chararray,
        classes_attended: int,
        classes_missed: int,
        attendance: float,
        grade_obtained: chararray,
        max_grade: chararray,
        status: chararray
    );
    
    
cleaned = FILTER raw BY (NOT (course MATCHES '.*HPC.*'));


avg_attendance = FOREACH (GROUP cleaned BY (course, program_name)) GENERATE
    group.course AS course,
    group.program_name AS program_name,
    AVG(cleaned.attendance) AS average_attendance;
    
    
ranked_data = FOREACH (GROUP avg_attendance BY (program_name)) {
    sorted = ORDER avg_attendance BY average_attendance DESC;
    top_3 = LIMIT sorted 3;
    
    GENERATE
        FLATTEN(top_3.course) AS course,
        FLATTEN(top_3.program_name) as program_name,
        FLATTEN(top_3.average_attendance) as average_attendance;
};

dump ranked_data;
