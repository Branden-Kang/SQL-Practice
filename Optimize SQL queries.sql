--1. Don't use "select *"

select * from employee;

select id,name from employee;

--2. If there is only one query result, use limit 1

CREATE TABLE employee (
id int(11) NOT NULL,
name varchar(255) DEFAULT NULL,
age int(11) DEFAULT NULL,
date datetime DEFAULT NULL,
sex int(1) DEFAULT NULL,
PRIMARY KEY (`id`) );

select id，name from employee where name='jay';

select id，name from employee where name='jay' limit 1;

-- 3. Avoid using or in the where clause
CREATE TABLE `user` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `userId` int(11) NOT NULL,
  `age` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_userId` (`userId`) )
  
select * from user where userid = 1 or age = 18;

select * from user where userid=1 
union all 
select * from user where age = 18;

-- 4. Optimize limit paging
select id, name, age from employee limit 10000, 10;

select id，name from employee where id>10000 limit 10;

-- 5. Opimize "like" statement
select userId，name from user where userId like '%123';

select userId，name from user where userId like '123%';

-- 6. Use where conditions to limit the data to be queried

--List<Long> userIds = sqlMap.queryList("select userId from user where isVip=1");
--boolean isVip = userIds.contains(userId);

--Long userId = sqlMap.queryObject("select userId from user where userId='userId' and isVip='1' ")
--boolean isVip = userId！=null;

--7. Avoid using != or <> operator
select age, name from user where age <> 18;

select age, name from user where age <18;
select age, name from user where age >18;

--8. Use bulk insertion if you insert too much data
'''
for(User u :list){
 INSERT into user(name,age) values(#name#,#age#)   
}
'''
'''
insert into user(name,age) values
<foreach collection="list" item="item" index="index" separator=",">
    (#{item.name},#{item.age})
</foreach>
'''

--9. Use distint keyword carefully
select distinct * from user;
select distinct name from user;
-- CPU time are higher with distinct

--10. Remove redundant and duplicate indexes
'''
KEY `idx_userId` (`userId`)  
KEY `idx_userId_age` (`userId`,`age`)
'''
'''
KEY `idx_userId_age` (`userId`,`age`)
'''

--11. If there is large data, optimize your modify/delect statement
delete from user where id <100000;
for（User user：list）{
   delete from user； }
   
delete user where id<500;
delete product where id>=500 and id<1000；

--12. Use default values instead of null
select * from user where age is not null;

select * from user where age>0;

--13. Replace union with union all
select * from user where userid=1 
union  
select * from user where age = 10

select * from user where userid=1 
union all  
select * from user where age = 10

--14. Use numeric fields, not character type
`king_id` varchar（20） NOT NULL;

`king_id` int(11) NOT NULL;

--15. Use varchar/navarchar instead of char/nchar
`deptName` char(100) DEFAULT NULL

`deptName` varchar(100) DEFAULT NULL

--16. Use explain to analyze your SQL plan
explain select * from user where userid = 10086 or age =18;
