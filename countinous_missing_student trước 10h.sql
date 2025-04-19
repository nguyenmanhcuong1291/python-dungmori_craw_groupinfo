SELECT 
    max(date_attendance) as date_attendance,
    group_id, 
    student_id,
    COUNT(*) AS continuous_missing_count
FROM (
    SELECT 
        id, 
        group_id, 
        student_id, 
        status,
        date_attendance,
        SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END) 
            OVER (PARTITION BY group_id, student_id ORDER BY id DESC) AS grp
    FROM (
       SELECT  
            s.id, 
            s.course_time_id, 
            s.student_id, 
            s.status, 
            s.absence_count, 
            c.group_id,
            c.date_attendance
        FROM course_time_students_tbl s
        JOIN course_times_tbl c ON s.course_time_id = c.id
        WHERE c.group_id IN (
            SELECT group_id
            FROM course_times_tbl 
            where group_id is not null and date_attendance =    CURRENT_DATE() 
        )  

        AND s.student_id IN (
            SELECT student_id
            FROM course_time_students_tbl s  
            JOIN course_times_tbl c ON s.course_time_id = c.id  
            WHERE c.date_attendance =                           CURRENT_DATE() 
        )
        AND c.date_attendance <                                 CURRENT_DATE() + INTERVAL 1 DAY
        AND c.date_attendance >= DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY) 
        ORDER BY s.id DESC

    ) AS filtered_data
) AS subquery
WHERE grp = 0
GROUP BY group_id, student_id
HAVING continuous_missing_count >2
