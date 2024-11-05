SELECT group_id,count(*)
from course_times_tbl
where group_id > 0 
and date_attendance  < current_date()
group by group_id 