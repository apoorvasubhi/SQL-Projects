-- DATA CLEANING --

SELECT * 
FROM layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * 
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- REMOVE DUPLICATES--

SELECT * ,
ROW_NUMBER() OVER (PARTITION BY
company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging
WHERE row_num > 1;
  
WITH duplicate_cte AS
(
SELECT * ,
ROW_NUMBER() OVER (PARTITION BY 
company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'hibob';

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL , 
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT * ,
ROW_NUMBER() OVER (PARTITION BY
company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
FROM layoffs_staging ;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;

-- Standardizing Data--
SELECT company,trim(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'crypto%';

SELECT DISTINCT country 
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%';

SELECT DISTINCT country , TRIM(TRAILING '.' FROM country) 
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country) 
WHERE country LIKE 'United States%';

SELECT `date`,str_to_date(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`,'%m/%d/%Y') ;

SELECT `date`
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` date;

-- NULL VALUES OR BLANK VALUES--

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry ='';

SELECT * 
FROM layoffs_staging2
WHERE company = 'airbnb';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL;

SELECT T1.industry,T2.industry 
FROM layoffs_staging2 T1
JOIN layoffs_staging2 T2
ON T1.company = T2.company
 WHERE T1.industry IS NULL 
 AND T2.industry IS NOT NULL ;
 
 UPDATE layoffs_staging2 T1
 JOIN layoffs_staging2 T2
     ON T1.company = T2.company
 SET T1.industry = T2.industry
WHERE T1.industry IS NULL 
 AND T2.industry IS NOT NULL ;

SELECT * 
FROM layoffs_staging2
WHERE company LIKE 'Bally%'; 

SELECT *
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

-- REMOVE ANY COLUMNS OR ROWS --

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;


-- EXPLORATORY DATA ANALYSIS --

SELECT *
FROM layoffs_staging2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT company,sum(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`),MAX(`date`)
FROM layoffs_staging2;

SELECT industry,sum(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT country,sum(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT *
FROM layoffs_staging2;

SELECT `date`,sum(total_laid_off)
FROM layoffs_staging2
GROUP BY `date`
ORDER BY 1 DESC;

SELECT YEAR(`date`),sum(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT stage,sum(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

SELECT SUBSTRING(`date`,6,2) AS `MONTH`,sum(total_laid_off)
FROM layoffs_staging2
GROUP BY `MONTH`
ORDER BY 2 DESC;

SELECT SUBSTRING(`date`,1,7) AS `MONTH`,sum(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`,1,7) AS `MONTH`,sum(total_laid_off) AS Total_Off
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`,Total_Off ,sum(total_off) OVER (ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;

SELECT COMPANY,YEAR(`date`),sum(total_laid_off)
FROM layoffs_staging2
GROUP BY COMPANY,YEAR(`date`)
ORDER BY 3 DESC;

WITH Company_Year(company,years,total_laid_off)AS
(
SELECT COMPANY,YEAR(`date`),sum(total_laid_off)
FROM layoffs_staging2
GROUP BY COMPANY,YEAR(`date`)
), Company_Year_Rank AS 
(
SELECT * ,DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
where years IS NOT NULL
)
SELECT * 
FROM Company_Year_Rank
where Ranking <=5;