# Workflow-Coffee-Bean_Analysis
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
## **Description**

This project established an automation process from retrieving data from PostgreSQL, performing data cleaning on Python, then visualizing on Elasticsearch.
Using Apache Airflow (refer to the DAG file), the process could be scheduled with desired period (daily at specific time).

For presentation slide, you can access [here](https://drive.google.com/drive/u/2/folders/11ktcq7FBvrHB9wiTmP8ZSdIOu1FNtFA9).

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
## **Data Source**

The data was taken from Coffee Quality Institute (CQI), a non-profit organization dedicating their works on improving coffee quality and value worldwide.
It's available until May 2023 and accessible through this link: [Link to Dataset](https://www.kaggle.com/datasets/fatihb/coffee-quality-data-cqi)

In addition, the term cup score comes from total rating from 1-10 accross 7 categories (flavor, body, balance, aftertaste, aroma, acidity, and clean cup).
Therefore, the highest score should be 70.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
## **Data Cleaning and Analysis Details**  
In Python, there were several steps of data cleaning including: 
1. unifying column name formats,
2. handling missing values by replacing with mark,
3. string manipulation (e.g: removal of units such as kg), then conversion into other data type (str -> float)
4. string manipulation for making consistent categories (name of countries and coffee bean colors)

Data analysis was conducted on ElasticSearch, by showing visualization.

## **Results**
Some key findings were summarized as below:
* Top 5 main coffee producing countries: Ethiopia, Brazil, Guatemala, Honduras, and Colombia.
* Most common coffee processing method was washed / wet (60.2%)
* Best coffee bean varieties based on cup score: castillo, red bourbon, gesha, ethiopian heirlooms, and blend varieties of wolishalo, kurume, and dega.
* Coffee beans with darker brown shades have higher cup points than lighter-bluish shades.
* The range of altitudes where the coffee grew was from 0 to 2,400 m above sea level with some outliers beyond 4,600 m. The most common altitude was between 1,200 and 1,400 m.
  



