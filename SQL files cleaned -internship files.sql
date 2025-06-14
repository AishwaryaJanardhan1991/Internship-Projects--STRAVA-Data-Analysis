create database fitness_project;
use fitness_project;

##1) creating table for Daily_Activity
create table daily_activity (
    Id bigint,
    ActivityDay Date,
    TotalSteps int,
    TotalDistance float,
    TrackerDistance float,
    LoggedActivitiesDistance float,
    VeryActiveDistance float,
    ModeratelyActiveDistance float,
    LightActiveDistance float,
    SedentaryActiveDistance float,
    VeryActiveMinutes int,
    FairlyActiveMinutes int,
    LightlyActiveMinutes int,
    SedentaryMinutes int,
    Calories int
);

desc daily_activity;

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Project files internship/dailyActivity_merged_fixed.csv"
into table daily_activity
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

select * from daily_activity;

# Basic data cleaning
# Removing Duplicates

set sql_safe_updates = 0;

delete from daily_activity
where Id in (
select Id
from(
select Id, ActivityDay, count(*)
from daily_activity
group by Id, Activityday
Having count(*) > 1 )
as duplicate
);

# checking for null values
select * from daily_activity where Id is null or ActivityDay is null;

# check columns with all 0s
select distinct SedentaryActiveDistance, LoggedActivitiesDistance  from daily_activity; 

select count(*) as total_rows,
       sum(case when SedentaryActiveDistance = 0 then 1 else 0 end) as zero_sedentary_count,
       sum(case when LoggedActivitiesDistance = 0 then 1 else 0 end) as zero_logged_count from daily_activity;
-- checked for two cloumns whether too many '0' values there, or need to delete columns now.alter


select *
from daily_activity
where SedentaryActiveDistance <> 0 or LoggedActivitiesDistance <> 0;

select * from daily_activity; -- data cleaning done(saving new table as csv file)

##2) creating table for dailyCalories
create table daily_calories(
Id Bigint, 
ActivityDay varchar(20),
calories int);

desc daily_calories;

# loading data file

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Project files internship/dailyCalories_merged.csv"
into table daily_calories
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
Ignore 1 rows;

select * from daily_calories;

alter table daily_calories modify column ActivityDay date;
update daily_calories
set ActivityDay = STR_TO_DATE(ActivityDay, '%m/%d/%Y');

alter table daily_calories modify ActivityDay date;


#checking for duplicates
select ID, ActivityDay, count(*) from daily_calories
group by Id, ActivityDay
having count(*) >1;

# removing calory value = 0
delete from daily_calories where calories = 0;

select * from daily_calories;
-- checked for Daily Calories, null and duplicate, saving as csv file


##3) creating table for Dailyintensities
  create table daily_intensities(
  Id bigint,
    ActivityDay varchar(20),
    SedentaryMinutes int,
    LightlyActiveMinutes float,
	FairlyActiveMinutes int,
	veryActiveMinutes int,
    SedentaryActiveDistance float,
    LightActiveMinutes int,
    ModeratelyActiveDistance float,
    VeryActiveDistance float);
desc daily_intensities;

set sql_safe_updates = 0;
load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Project files internship/dailyIntensities_merged.csv"
into table daily_intensities
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
Ignore 1 rows;

select * from daily_intensities;  -- checking for the 0 count values
select SedentaryActiveDistance, count(*)
from daily_activity
group by SedentaryActiveDistance
order by SedentaryActiveDistance;

delete from daily_intensities
where Id in (
select Id
from(
select Id, ActivityDay, count(*)
from daily_intensities
group by Id, Activityday
Having count(*) > 1 )
as duplicate
);  -- checked for duplicates and no duplicates found 

# checking for null values
select * from daily_intensities where Id is null or ActivityDay is null;

select * from daily_intensities; -- all cleaning done saving as csv file to send to python

##4) Creating table Daily Steps
create table daily_steps(
Id Bigint, 
ActivityDay date,
StepTotal int);

desc daily_steps;
# loading data file

set sql_safe_updates = 0;

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Project files internship/dailySteps_merged 1_fixed final.csv"
into table daily_steps
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
Ignore 1 rows;

select * from daily_steps;
#checking for duplicates
select ID, ActivityDay, count(*) from daily_steps
group by Id, ActivityDay
having count(*) >1;

delete from daily_steps
where Id in (
select Id
from(
select Id, ActivityDay, count(*)
from daily_steps
group by Id, Activityday
having count(*) > 1 )
as duplicate
); --  no duplicates found

# checking for null values
select * from daily_steps where Id is null or ActivityDay is null; -- no null values found
select * from daily_steps;    -- (saving as csv file)

##5) creating table for Hourly Calories
create table hourly_calories(
Id Bigint, 
ActivityHour datetime,
Calories int);

desc hourly_calories;

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Project files internship/hourlyCalories_merged(in).csv"
into table hourly_calories
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
Ignore 1 rows;

select * from hourly_calories;

select ID, ActivityHour, count(*) from hourly_calories
group by Id, ActivityHour
having count(*) >1;
set sql_safe_updates = 0;

delete from hourly_calories
where Id in (
select Id
from(
select Id, ActivityHour, count(*)
from hourly_calories
group by Id, ActivityHour
Having count(*) > 1 )
as duplicate
);  
# checking for null values
select * from hourly_calories where Id is null or ActivityHour is null; -- no null values found
select* from hourly_calories; -- removed large number of duplicates(each raw had 24 duplicated values)
-- cleaned data saved as csv file


##6) creating table for Hourly Steps
create table hourly_steps(
  Id bigint,
    ActivityHour datetime,
    StepTotal int);
    
desc hourly_steps;

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Project files internship/hourlySteps_merged 2(in).csv"
into table hourly_steps
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
Ignore 1 rows;

select * from hourly_steps;

select ID, ActivityHour, count(*) from hourly_steps
group by Id, ActivityHour
having count(*) >1; -- checked for duplicates, it having 24 count in each raw

select sum(dup_count) - count(*) as total_duplicates
from (
    select count(*) as dup_count
    from hourly_steps
    group by Id, ActivityHour, StepTotal
    having count(*) > 1
) as duplicates; 

with RankedSteps as (
    select *,
           row_number() over (partition by Id, ActivityHour, StepTotal order by Id) as rn
    from hourly_steps
)

-- Step 2: Delete duplicates (keep only rn = 1)
delete from hourly_steps
where (Id, ActivityHour, StepTotal) in (
    select Id, ActivityHour, StepTotal
    from RankedSteps
    where rn > 1
); -- all duplicates got removed

select * from hourly_steps;
# checking for null values
select * from hourly_steps where Id is null or ActivityHour is null; --  no null observed
  
##5) creating table for hourlyintensities

   create table hourlyIntensities(
Id Bigint, 
ActivityHour datetime,
TotalIntensity int,
AverageIntensity int);

desc hourlyIntensities;

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Project files internship/hourlyIntensities_merged(in).csv"
into table hourlyIntensities
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
Ignore 1 rows;  

select * from hourlyIntensities;
     
select ID, ActivityHour, count(*) from hourlyIntensities
group by Id, ActivityHour
having count(*) >1;     
         
 
select *
from hourlyIntensities
where Id not in (
    select MIN(Id)
    from hourlyIntensities
    group by Id, ActivityHour, TotalIntensity, AverageIntensity
);  -- ch	ecked for duplicates, duplicates are found


select count(*) as total_records from hourlyIntensities; -- checked for total feeds in data

select 
    min(TotalIntensity) as min_intensity,
    max(TotalIntensity) as max_intensity,
    avg(TotalIntensity) as avg_intensity
from hourlyIntensities;   -- checked for min, max and average of totalintensity

select sum(dup_count) - count(*) as total_duplicates
from (
    select count(*) as dup_count
    from hourlyIntensities
    group by Id, ActivityHour, TotalIntensity, AverageIntensity
    having count(*) > 1
) as duplicates;   -- having some duplicates

delete from hourlyIntensities
where Id not in (
    select * from (
        select min(Id)
        from hourlyIntensities
        group by Id, ActivityHour, TotalIntensity, AverageIntensity
    ) as keep_rows
);  -- trying to delete duplicates

select count(*) as remaining_duplicates
from (
    select count(*) as dup_count
    from hourlyIntensities
    group by Id, ActivityHour, TotalIntensity, AverageIntensity
    having count(*) > 1
) as duplicates;

alter table hourlyIntensities add column row_id int auto_increment primary key;
select Id, ActivityHour, TotalIntensity, AverageIntensity, count(*) as dup_count
from hourlyIntensities
group by Id, ActivityHour, TotalIntensity, AverageIntensity
having count(*) > 1;

delete from hourlyIntensities
where row_id not in (
    select row_id from (
        select min(row_id) as row_id
        from hourlyIntensities
        group by Id, ActivityHour, TotalIntensity, AverageIntensity
    ) as keep_rows
);

select * from hourlyIntensities;
select 
    hour(ActivityHour) as hour_of_day,
    avg(TotalIntensity) as avg_intensity
from hourlyIntensities
group by hour_of_day
order by hour_of_day;

select 
    Id,
    sum(TotalIntensity) as total_intensity
from hourlyIntensities
group by Id
order by total_intensity desc
limit 10;

select 
    dayname(ActivityHour) as day_of_week,
    avg(TotalIntensity) as avg_intensity
from hourlyIntensities
group by day_of_week
order by field(day_of_week, 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday');

select count(*) as inactive_hours
from hourlyIntensities
where TotalIntensity = 0;

select 
    Id,
    date(ActivityHour) as activity_date,
    sum(TotalIntensity) as daily_total_intensity
from hourlyIntensities
group by Id, activity_date
order by activity_date;

select * from hourly_steps where Id is null or ActivityHour is null; -- checked for null, no null obseved. savindg file for further python cleaning.