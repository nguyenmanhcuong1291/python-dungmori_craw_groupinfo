SELECT
						g.id as group_id
						,g.name
						,g.type
						,g.vip_level
						,g.vip_session
						,g.shift_type
						,g.shift_time
						,g.start_date
						,date(g.expired_at) as expired_date
						,gu_temp.count_students
					FROM
					community_groups g
					join (
							SELECT
								gu.group_id,count(*) as count_students
							FROM
								users u 
								INNER JOIN community_group_users gu ON u.id = gu.user_id and u.is_tester <> 1 
							GROUP BY gu.`group_id`
								having count(*) > 0
						) gu_temp on gu_temp.group_id = g.id

					WHERE g.start_date BETWEEN '2024-01-01' AND CURRENT_DATE and g.expired_at >= now()