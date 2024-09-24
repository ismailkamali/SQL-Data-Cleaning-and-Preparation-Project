-- DATA CLEANING PROJECT by Ismail Kamali
-- We will be cleaning and preparing data for a follow up data analysis project

SELECT *
FROM layoffs;

-- STEP 0: Staging process

-- STEP 1: Remove duplicates if there are any

-- STEP 2: Standardise data

-- STEP 3: Address NULL/blank values

-- STEP 4: Remove any columns if necessary

-- STEP 0

CREATE TABLE layoffs_staging
LIKE layoffs; #Creating a new table with the same column names as our raw data so we can work on a different table without editing our raw data

INSERT layoffs_staging
SELECT *
FROM layoffs; #Now we are copying all our data into our new table to create a staged data set

SELECT *
FROM layoffs_staging; #Checking it has worked

-- STEP 1: Removing duplicates

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off,`date`) AS row_num
FROM layoffs_staging; #Here we are checking if a row number over a partition with every column will display duplicate entries

-- We have slightly changed the partition in the CTE below as it ended up ranking very similar but not duplicated entries

WITH duplicate_CTE AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_CTE
WHERE row_num > 1; #Now we have created a CTE so we can query it on row_numbers greater than 1. Anything with a row number >1 indicates a duplicate entry

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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; #We have now created a new table with an extra column called row_num

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country,funds_raised_millions) AS row_num
FROM layoffs_staging; #We have now inserted our row numbers over our partition to create a table which indicates duplicates using row numbers

DELETE
FROM layoffs_staging2
WHERE row_num > 1; #Here we delete all duplicate values

SELECT *
FROM layoffs_staging2
WHERE row_num > 1; #Checking it has worked

-- STEP 2: Standardising data to prepare it for our next project

SELECT *
FROM layoffs_staging2; #Investigating our dataset

SELECT company, TRIM(company)
FROM layoffs_staging2; #Investigating if a trim function will standardise our company column

UPDATE layoffs_staging2
SET company = TRIM(company); #Carrying out our standardisation

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'; #Now since there are multiple names for the crypto industry, we will update all of these to just crypto

SELECT distinct industry
FROM layoffs_staging2
ORDER BY 1; #Checking if there is only one industry for crypto

SELECT DISTINCT country
FROM layoffs_staging2
WHERE country LIKE 'United States%'; #Another problem with a '.' after united states for an entry meaning that there's 2 different countries when it should be the same

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%'; #We are trimming the '.' off the end of 'United States' anywhere there is a '.' present

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging; #Investigating date formatting

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y'); #Correcting our date formatting

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE; #Converting our date datatype from text to date

SELECT *
FROM layoffs_staging2; #Checking if anything else needs standardising, and it doesn't!

-- STEP 3: Populating null values from the information we already have

SELECT industry
FROM layoffs_staging2
WHERE industry is NULL
OR industry = ''; #Investigating null and blank values in industry column to see if we can fix any

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = ''; #Changing blanks to NULL to make things easier

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb'; #CHecking what the industry Airbnb is under if there are any other rows of Airbnb

SELECT t1.industry,t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
	AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL; #Investigating if we can populate blank/null industries by using a join, it works!

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE (t1.industry is NULL)
AND t2.industry IS NOT NULL; #Here we use a join on 2 tables to update our industrys and match null values with actual values from existing companies with the same name and location

SELECT *
FROM layoffs_staging2
WHERE industry is NULL; #Investigating on if there are any remaining NULLs

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%'; #This null can't be populated due to there not being another company with the same name and location that can populate it

-- We can not populate the other values with our existing data

-- Step 4: Removing rows and columns that do not give any useful information

DELETE
FROM layoffs_staging2
WHERE total_laid_off is NULL 
AND percentage_laid_off is NULL; #These rows do not give us any useful information for our data analysis project

ALTER TABLE layoffs_staging2
DROP COLUMN row_num; #This column was only used for duplicates

SELECT *
FROM layoffs_staging2; #This is our cleaned data