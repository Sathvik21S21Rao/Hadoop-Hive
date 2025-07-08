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

dump cleaned;
