# Project Plan

## Title
Analyzing the Relationship Between Current Health Expenditure and GDP Growth in south America.

## Main Question

<!-- Think about one main question you want to answer based on the data. -->
How does Current health expenditure as a percentage of GDP influence economic growth in South American countries between 2014 and 2023?

## Description

<!-- Describe your data science project in max. 200 words. Consider writing about why and how you attempt it. -->
This project investigates the relationship between Current health expenditure as a percentage of GDP and economic growth in south America. Using World Bank data on health spending and GDP growth, the study analyzes trends and applies statistical method, including correlation model, to explore potential causality. By identifying how health investments impact economic performance, the research provides insights to guide policymakers in optimizing healthcare spending for sustainable growth.

## Datasources

<!-- Describe each datasources you plan to use in a section. Use the prefic "DatasourceX" where X is the id of the datasource. -->

### Datasource1: Current health expenditure (% of GDP) (Worldbank )
* Metadata URL: https://data.worldbank.org/indicator/SH.XPD.CHEX.GD.ZS
* Data URL: https://api.worldbank.org/v2/en/indicator/SH.XPD.CHEX.GD.ZS?downloadformat=csv
* Data Type: Zip -> CSV

The dataset includes global records of Current health expenditure as a share of GDP. For this study, the focus is narrowed to south America, extracting data from 2014 to 2023. This data is used to explore trends and analyze the impact of health spending on economic growth within the region.

### Datasource1: GDP growth (annual %)(Worldbank )
* Metadata URL: https://data.worldbank.org/indicator/NY.GDP.MKTP.KD.ZG
* Data URL: https://api.worldbank.org/v2/en/indicator/NY.GDP.MKTP.KD.ZG?downloadformat=csv
* Data Type: Zip -> CSV

This dataset provides annual GDP growth rates for countries across south America from 2014 to 2023. It serves as a key resource for analyzing economic performance and studying the relationship between GDP growth and health expenditure in the region

## Work Packages

<!-- List of work packages ordered sequentially, each pointing to an issue with more details. -->

1. Dataset Selection
2. Automating the Data Pipeline for Seamless Analysis
3. Exploratory Data Analysis (EDA) and Feature Engineering
4. Building & Tuning Predictive Models for Accuracy
5. Evaluating Models: Performance, Insights & Impact
6. Delivering Findings: Insights & Actionable Recommendations

<!-- [i1]: https://github.com/jvalue/made-template/issues/1 -->
