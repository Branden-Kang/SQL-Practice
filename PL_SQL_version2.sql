--Q1
--select to_char(instructor.ID,'00099') as instructorID, instructor.name as instructorName, 
--        to_char(advisor.s_id,'00999') as adviseeID, student.name as adviseeName
--from instructor, advisor, student
--where instructor.id = advisor.i_id and advisor.s_id = student.id and instructor.dept_name = 'Elec. Eng.'
--order by to_char(instructor.ID,'00099'), to_char(advisor.s_id,'00999');
create or replace procedure advlist(arg_deptname in instructor.dept_name%type) 
as
i_id instructor.ID%type; 
i_name instructor.name%type; 
a_id advisor.s_id%type; 
a_name student.name%type;
i integer;
cursor c (dname in instructor.dept_name%type) is
select to_number(instructor.ID,'99999') as instructorID, instructor.name as instructorName, 
        to_number(advisor.s_id,'99999') as adviseeID, student.name as adviseeName
from instructor, advisor, student
where instructor.id = advisor.i_id and advisor.s_id = student.id and instructor.dept_name = dname
order by to_number(instructor.ID,'99999'), to_number(advisor.s_id,'99999');
begin
select count(*) into i from student where dept_name = arg_deptname;
if i = 0 then
dbms_output.put_line('Department' || arg_deptname || 'does not exist');
return;
end if;
open c (arg_deptname);
loop
fetch c into i_id, i_name, a_id, a_name;
if c%rowcount = 0
then dbms_output.put_line('Department'||'  '||  arg_deptname ||'  '|| 'has no instructors');
--then dbms_output.put_line ('Empty result');
exit;
end if;
exit when c%notfound;
--dbms_output.put_line(rpad(i_id,5,' ') || rpad(i_name,5,' ') || rpad(a_id,5,' ') || rpad (a_name,5,' '));
dbms_output.put_line(i_id ||'  '|| i_name ||'  '|| a_id ||'  '|| a_name);
end loop;
end;

exec advlist('Elec. Eng.');

--Q2
create or replace trigger tr before delete on instructor 
referencing old as n
for each row
begin
delete from teaches
where teaches.id = :n.id; 
end;
--Q3
--As we know triggers always ran as part of transaction,
--When there is a failure in trigger and if we have "rollback transaction" then complete transaction will be rolled back and no modification/changes are made to the table.