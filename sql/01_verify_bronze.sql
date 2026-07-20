-- Databricks notebook source
SELECT
       city,
       parameter,
       count(*) as readings,
       round(avg(value), 2)  as avg_value,
       round(max(value), 2) as max_value,
       round(min(value), 2) as min_value
FROM aqipulse_catalog.aqipulse.bronze_air_quality
GROUP BY city, parameter
ORDER BY city, parameter;