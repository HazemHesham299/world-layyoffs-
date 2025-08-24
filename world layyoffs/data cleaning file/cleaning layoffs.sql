select * from layoffs ;
----- dublicate the data to work on it ---------------
create table layoffs1 as table layoffs ;
select * from layoffs1 ;

-------- remove doublicates -------------------- 
select * , 
row_number ()  over (partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions)
as row_num
from layoffs1 ;

with doublicate_cte as 
(
select * , 
row_number ()  over (partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage , country, funds_raised_millions)
as row_num
from layoffs1 
)
select * 
from doublicate_cte
where row_num >1 ;

------ let's just look at Hibob  to confirm-----

select * from layoffs1 
where company = 'Hibob' ;
---- delet the duplicates ----------------	
WITH duplicate_cte AS (
    SELECT ctid,
           ROW_NUMBER() OVER (
               PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions
           ) AS row_num
    FROM layoffs1
)
DELETE FROM layoffs1
WHERE ctid IN (
    SELECT ctid
    FROM duplicate_cte
    WHERE row_num > 1
);

select * from layoffs1;
----------- standardizing data --------------- 
select company , trim (company)
from layoffs1 ;

update layoffs1 
set company = trim(company);


select upper (company) 
from layoffs1 ;

SELECT *
FROM layoffs1
WHERE industry LIKE 'Crypto%';

update layoffs1
set industry = 'Crypto'
WHERE industry LIKE 'Crypto%';
-----3. Look at Null Values----------
------ if we look at industry it looks like we have some null and empty rows, let's take a look at these
select * from layoffs1 
where industry is null 
or industry = ''
order by industry ;

update  layoffs1
set industry = 'unknown'
 where industry  is null or industry = '' ;


DELETE FROM layoffs1
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


------------- start to Exploratory the  Data ------------------------
select * from layoffs1 ;
----------- MAX  of total_laid_off --------------------------  
select max(total_laid_off)
from layoffs1 ; 

-- Looking at Percentage to see how big these layoffs were --------------
select max(percentage_laid_off) , min(percentage_laid_off)
from layoffs1 
where percentage_laid_off is not null ;
---- Which companies had 1 which is basically 100 percent of they company laid off-----
select *
from layoffs1
where percentage_laid_off = 1  ;

--- if we order by funcs_raised_millions we can see how big some of these companies were
select *
from layoffs1
where percentage_laid_off = 1 
order by funds_raised_millions desc ;

--------- Companies with the biggest single Layoff ------------
select company , total_laid_off 
from layoffs1 
order by 2 desc 
limit 10 ;

------ by location ----------
select location , sum(total_laid_off) as sum_total_laid_off
from layoffs1 
group by location
order by 2 desc 
limit 10 ;

------ this is total in the past 3 years or in the dataset---------- 
select company , sum( total_laid_off ) as sum_total_laid_off 
from layoffs1 
group by company
order by 2 desc ;

select extract (year  from date) as year , 
sum( total_laid_off ) as sum_total_laid_off 
from layoffs1 
group by extract (year  from date)
order by 2 desc ;

select industry , sum( total_laid_off ) as sum_total_laid_off 
from layoffs1 
group by industry
order by 2 desc ;

select stage , sum( total_laid_off ) as sum_total_laid_off 
from layoffs1 
group by stage
order by 2 desc ;

--  we looked at Companies with the most Layoffs. Now let's look at that per year.

with company_year as (
select company , extract (year  from date) as years , 
sum( total_laid_off ) as sum_total_laid_off 
from layoffs1 
group by company , extract (year  from date)
)
, company_year_rank as (
select company , years ,  sum_total_laid_off  ,
dense_rank () over( partition by years order by sum_total_laid_off ) as ranking 
from company_year 
) 
select company , years , sum_total_laid_off , ranking 
from company_year_rank 
where ranking <= 3 
and years is not null 
order by years asc , sum_total_laid_off desc ; 

------ Rolling Total of Layoffs Per Month
SELECT to_char(date, 'yyyy-mm') AS months,
       SUM(total_laid_off) AS total_laid_off
FROM layoffs1
WHERE date IS NOT NULL
GROUP BY months
ORDER BY 1 DESC;

----------- total_layoffs for each month in each year ------------
WITH DATE_CTE AS (
SELECT to_char(date, 'yyyy-mm') AS months,
       SUM(total_laid_off) AS total_laid_off
FROM layoffs1
WHERE date IS NOT NULL
GROUP BY months
)
select months,SUM(total_laid_off) OVER (ORDER BY months ASC) as rolling_total_layoffs
from DATE_CTE
ORDER BY months ASC;

