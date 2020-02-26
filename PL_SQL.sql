--Q1
create or replace procedure teaching (arg id in instructor.id%type) as
dept course.dept name%type; crs course.course id%type; secid section.sec id%type; title course.title%type;
yr section.year%type;
sem section.semester%type;
i integer;
cursor c (iid in instructor.id%type) is
select dept name, course id, title, sec id, year, semester, count(*) as enrollment
from takes join
(select dept name, course id, sec id, title, year, semester from course natural join section natural join teaches
where id = iid) T using (course id, sec id, semester, year)
group by dept name, course id, title, sec id, year, semester
order by dept name, course id, year, semester desc;
begin
select count(*) into i from instructor where id = arg id;
if i = 0 then
dbms output.put line (’Instructor ’ —— arg id —— ’ does not exist’);
return;
end if;
open c (arg id);
loop
fetch c into dept, crs, title, secid, yr, sem, i;
exit when c%notfound;
dbms output.put line(rpad(dept,20,’ ’) —— rpad(crs,5,’ ’) —— rpad(title,30,’ ’) —— rpad (secid,4,’ ’) ——rpad(sem,10,’ ’)—— rpad(yr,7,’ ’) —— i);
end loop;
if c%rowcount = 0 then
dbms output.put line (’Instructor ’ —— arg id —— ’ has not taught any courses’);
end if;
end;
--Q2
create or replace trigger tr after delete on instructor referencing old as n
for each row
begin
update instructor
set id = null
where id = :n.id; end;
--Q3
--As we know triggers always ran as part of transaction,
--When there is a failure in trigger and if we have "rollback transaction" then complete transaction will be rolled back and no modification/changes are made to the table.
