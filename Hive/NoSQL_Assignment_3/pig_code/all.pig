-- query 1:


--------------------------------------------

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


avg_attendance_by_course = foreach (group cleaned by (course, program_name)) GENERATE 
    group.course as course, 
    group.program_name as program_name, 
    AVG(cleaned.attendance) AS avg_attendance;
  
    
grade_counts = foreach (group cleaned by (course, program_name, grade_obtained)) GENERATE 
    group.course as course, 
    group.program_name as program_name, 
    group.grade_obtained as obtained_grade, 
    COUNT(cleaned) AS grade_count;


mode_grade = FOREACH (GROUP grade_counts BY (course, program_name)) {
    sorted_grades = ORDER grade_counts BY grade_count DESC, obtained_grade ASC;
    top_grade = LIMIT sorted_grades 1;
    GENERATE
        top_grade.course AS course,
        top_grade.program_name AS program_name,
        top_grade.obtained_grade AS mode_grade;
};

mode_grade = FOREACH (GROUP grade_counts BY (course, program_name)) {
     sorted_grades = ORDER grade_counts BY grade_count DESC, obtained_grade ASC;
     top_grade = LIMIT sorted_grades 1;
     GENERATE
         FLATTEN(top_grade.course) AS course,
         FLATTEN(top_grade.program_name) AS program_name,
         FLATTEN(top_grade.obtained_grade) AS mode_grade;
};


joined_results = JOIN avg_attendance_by_course BY (course, program_name),
                    mode_grade BY (course, program_name);


final_result = FOREACH joined_results GENERATE
    avg_attendance_by_course::course AS course,
    avg_attendance_by_course::program_name AS program_name,
    avg_attendance_by_course::avg_attendance AS avg_attendance,
    mode_grade::mode_grade AS mode_grade;


dump final_result;

-- query 2:


--------------------------------------------







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


-- query 3:


--------------------------------------------






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
