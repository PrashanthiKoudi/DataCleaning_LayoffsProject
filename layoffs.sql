#Data Cleaning

#NOTE : In this Project I will be covering below points
#	1. Remove Duplicates
# 	2. Standarize data
#	3. Handle Null or blank values
#	4. Remove unwanted columns or rows

select count(*) as total from layoffs;

CREATE table layoffs_staging
LIKE layoffs;

INSERT into layoffs_staging
SELECT * FROM layoffs;

SELECT * from layoffs_staging
WHERE company =' E Inc.';

# Checking if duplicates exists

SELECT *,
ROW_NUMBER() OVER( PARTITION BY company, location, industry, total_laid_off,`date`) AS row_num
FROM layoffs_staging
ORDER BY row_num;
    
WITH duplicate_CTE AS 
(
SELECT *,
	ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoffs_staging
)
SELECT *
FROM duplicate_CTE
WHERE row_num > 1;

#Since we cannot delete a row on a from a CTE and also referring to a row which is generated dynamically in CTE we go with creating a temp table and deleting from it.

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging;
        
select * from layoffs_staging2
WHERE row_num >=2;

DELETE FROM layoffs_staging2
WHERE row_num > 1;


# Standardizing data
SET SQL_SAFE_UPDATES = 0;
UPDATE layoffs_staging2
SET company = TRIM(company);

UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

SELECT country
FROM layoffs_staging2
WHERE country LIKE '%nited%';

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

#working on Null and blank spaces

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company='Airbnb';

#now we need to populate those nulls if possible

UPDATE layoffs_staging2
SET industry= NULL
WHERE industry='';

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;


SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM world_layoffs.layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

