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


attendance_stats = FOREACH (GROUP cleaned BY (course, program_name)) {
    
    total_count = COUNT(cleaned);
    
    below_50 = FILTER cleaned BY attendance < 50.0;
    below_50_count = COUNT(below_50);
    
    percentage = (below_50_count * 100.0) / total_count;
    
    GENERATE
        group.course AS course,
        group.program_name AS program_name,
        percentage AS percentage_below_50_attendance;
};

sorted_results = ORDER attendance_stats BY course, program_name;


dump sorted_results;
