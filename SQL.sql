--Q1
select distinct c.course_id, c.title, s.dept_name, c.dept_name
from student s, course c, department d
where s.dept_name <> c.dept_name and c.dept_name <> d.dept_name and s.id=92557
order by c.course_id;
--Q2
select dept_name
from department
where budget > some(select budget*1.1 from department where dept_name='History')
order by dept_name;
--Q2 Different version
select distinct D.dept_name
from department D, department H
where H.dept_name = 'History' and D.budget >= 1.1 * H.budget order by dept_name;
--Q3
select id, name, credits, tot_cred
  from
( select to_char(s.id,'00009') as id, s.name, sum(c.credits) as credits, s.tot_cred --, c.dept_name
from student s, course c
where s.dept_name = c.dept_name and s.dept_name = 'Physics'
group by s.id, s.name, s.tot_cred --count(c.credits)
order by s.id asc)
where ROWNUM <= 10
order by id asc;
--Q3 Different version
select to_char(s.id,'00009') as cid, s.name, count(c.credits) as credits, s.tot_cred --, c.dept_name
from student s, course c, takes t
where c.course_id = t.course_id and s.dept_name = c.dept_name and s.dept_name = 'Physics'
group by s.id, s.name, s.tot_cred --count(c.credits)
order by s.id asc;
--Q3 Different version
select to_char(ID,'00009') as ID, name,
(select sum(credits) from course natural join takes where ID = student.ID) as credits, tot_cred
from student
where dept_name = 'Physics'
order by ID;
--Q4
select course_id as CID
  from
( select course_id, count(sec_id)
from section
group by course_id
order by count(sec_id) desc)
where ROWNUM <= 3
order by course_id asc;
--Q4 Different version
select course_id as CID
from section
group by course_id
having count(*) >=all (select count(*) from section group by course_id) order by CID;
--Q5
select course_id as CID, c.title
  from
(select course_id, count(s.id)
from student s, takes t
where s.id = t.id
group by course_id
having count(s.id) > 400
order by course_id) natural join course c
order by course_id
;
--Q5 Different version
select course_id as CID, title from course
where course_id in (
select course_id
from takes
group by course_id having count(*) > 400)
order by CID;
--Q6
select distinct c.course_id as CID
from course c, section s, classroom r
where r.capacity > some(select capacity
                    from classroom
                    where capacity>130);
--Q6 Different version
select course_id as CID from course
where not exists(
select *
from classroom
where capacity > 130 and not exists (
select *
from section
where course_id = course.course_id and building = classroom.building));
--Q7
select to_char(avg(salary),'99999.99') as avgsal from instructor; -- 81491.37
select to_char(sum(salary)/count(*),'99999.99') as avgsal from instructor; -- 80684.52
--Answer: avg and count functions are operating differently in null values
--Because there is null value in this table, they are the different.
--Q8
select course_id
from course
where course_id not in (select course_id from takes)
order by course_id;
--Q9
select course_id
from course natural left outer join takes
where sec_id is null
order by course_id;
--Q10
select course_id
from course
minus
select course_id
from takes;
