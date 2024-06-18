WITH stg_customers AS (

  SELECT * 
  
  FROM {{ ref('stg_customers')}}

),

Reformat_1_2 AS (

  SELECT 
    CUSTOMER_ID AS CUSTOMER_ID0,
    FIRST_NAME AS FIRST_NAME0,
    LAST_NAME AS LAST_NAME0,
    CUSTOMER_ID AS CUSTOMER_ID1,
    FIRST_NAME AS FIRST_NAME1,
    LAST_NAME AS LAST_NAME1,
    CUSTOMER_ID AS CUSTOMER_ID2,
    FIRST_NAME AS FIRST_NAME2,
    LAST_NAME AS LAST_NAME2,
    CUSTOMER_ID AS CUSTOMER_ID3,
    FIRST_NAME AS FIRST_NAME3,
    LAST_NAME AS LAST_NAME3,
    CUSTOMER_ID AS CUSTOMER_ID4,
    FIRST_NAME AS FIRST_NAME4,
    LAST_NAME AS LAST_NAME4,
    CUSTOMER_ID AS CUSTOMER_ID5,
    FIRST_NAME AS FIRST_NAME5,
    LAST_NAME AS LAST_NAME5,
    CUSTOMER_ID AS CUSTOMER_ID6,
    FIRST_NAME AS FIRST_NAME6,
    LAST_NAME AS LAST_NAME6,
    CUSTOMER_ID AS CUSTOMER_ID7,
    FIRST_NAME AS FIRST_NAME7,
    LAST_NAME AS LAST_NAME7,
    CUSTOMER_ID AS CUSTOMER_ID8,
    FIRST_NAME AS FIRST_NAME8,
    LAST_NAME AS LAST_NAME8,
    CUSTOMER_ID AS CUSTOMER_ID9,
    FIRST_NAME AS FIRST_NAME9,
    LAST_NAME AS LAST_NAME9,
    CUSTOMER_ID AS CUSTOMER_ID10,
    FIRST_NAME AS FIRST_NAME10,
    LAST_NAME AS LAST_NAME10,
    CUSTOMER_ID AS CUSTOMER_ID11,
    FIRST_NAME AS FIRST_NAME11,
    LAST_NAME AS LAST_NAME11,
    CUSTOMER_ID AS CUSTOMER_ID12,
    FIRST_NAME AS FIRST_NAME12,
    LAST_NAME AS LAST_NAME12,
    CUSTOMER_ID AS CUSTOMER_ID13,
    FIRST_NAME AS FIRST_NAME13,
    LAST_NAME AS LAST_NAME13,
    CUSTOMER_ID AS CUSTOMER_ID14,
    FIRST_NAME AS FIRST_NAME14,
    LAST_NAME AS LAST_NAME14,
    CUSTOMER_ID AS CUSTOMER_ID15,
    FIRST_NAME AS FIRST_NAME15,
    LAST_NAME AS LAST_NAME15,
    CUSTOMER_ID AS CUSTOMER_ID16,
    FIRST_NAME AS FIRST_NAME16,
    LAST_NAME AS LAST_NAME16,
    CUSTOMER_ID AS CUSTOMER_ID17,
    FIRST_NAME AS FIRST_NAME17,
    LAST_NAME AS LAST_NAME17,
    CUSTOMER_ID AS CUSTOMER_ID18,
    FIRST_NAME AS FIRST_NAME18,
    LAST_NAME AS LAST_NAME18,
    CUSTOMER_ID AS CUSTOMER_ID19,
    FIRST_NAME AS FIRST_NAME19,
    LAST_NAME AS LAST_NAME19,
    FIRST_NAME AS FIRST_NAME,
    LAST_NAME AS LAST_NAME,
    CUSTOMER_ID AS CUSTOMER_ID
  
  FROM stg_customers

),

Reformat_1_1_1 AS (

  SELECT 
    CUSTOMER_ID AS CUSTOMER_ID0,
    FIRST_NAME AS FIRST_NAME0,
    LAST_NAME AS LAST_NAME0,
    CUSTOMER_ID AS CUSTOMER_ID1,
    FIRST_NAME AS FIRST_NAME1,
    LAST_NAME AS LAST_NAME1,
    CUSTOMER_ID AS CUSTOMER_ID2,
    FIRST_NAME AS FIRST_NAME2,
    LAST_NAME AS LAST_NAME2,
    CUSTOMER_ID AS CUSTOMER_ID3,
    FIRST_NAME AS FIRST_NAME3,
    LAST_NAME AS LAST_NAME3,
    CUSTOMER_ID AS CUSTOMER_ID4,
    FIRST_NAME AS FIRST_NAME4,
    LAST_NAME AS LAST_NAME4,
    CUSTOMER_ID AS CUSTOMER_ID5,
    FIRST_NAME AS FIRST_NAME5,
    LAST_NAME AS LAST_NAME5,
    CUSTOMER_ID AS CUSTOMER_ID6,
    FIRST_NAME AS FIRST_NAME6,
    LAST_NAME AS LAST_NAME6,
    CUSTOMER_ID AS CUSTOMER_ID7,
    FIRST_NAME AS FIRST_NAME7,
    LAST_NAME AS LAST_NAME7,
    CUSTOMER_ID AS CUSTOMER_ID8,
    FIRST_NAME AS FIRST_NAME8,
    LAST_NAME AS LAST_NAME8,
    CUSTOMER_ID AS CUSTOMER_ID9,
    FIRST_NAME AS FIRST_NAME9,
    LAST_NAME AS LAST_NAME9,
    CUSTOMER_ID AS CUSTOMER_ID10,
    FIRST_NAME AS FIRST_NAME10,
    LAST_NAME AS LAST_NAME10,
    CUSTOMER_ID AS CUSTOMER_ID11,
    FIRST_NAME AS FIRST_NAME11,
    LAST_NAME AS LAST_NAME11,
    CUSTOMER_ID AS CUSTOMER_ID12,
    FIRST_NAME AS FIRST_NAME12,
    LAST_NAME AS LAST_NAME12,
    CUSTOMER_ID AS CUSTOMER_ID13,
    FIRST_NAME AS FIRST_NAME13,
    LAST_NAME AS LAST_NAME13,
    CUSTOMER_ID AS CUSTOMER_ID14,
    FIRST_NAME AS FIRST_NAME14,
    LAST_NAME AS LAST_NAME14,
    CUSTOMER_ID AS CUSTOMER_ID15,
    FIRST_NAME AS FIRST_NAME15,
    LAST_NAME AS LAST_NAME15,
    CUSTOMER_ID AS CUSTOMER_ID16,
    FIRST_NAME AS FIRST_NAME16,
    LAST_NAME AS LAST_NAME16,
    CUSTOMER_ID AS CUSTOMER_ID17,
    FIRST_NAME AS FIRST_NAME17,
    LAST_NAME AS LAST_NAME17,
    CUSTOMER_ID AS CUSTOMER_ID18,
    FIRST_NAME AS FIRST_NAME18,
    LAST_NAME AS LAST_NAME18,
    CUSTOMER_ID AS CUSTOMER_ID19,
    FIRST_NAME AS FIRST_NAME19,
    LAST_NAME AS LAST_NAME19,
    FIRST_NAME AS FIRST_NAME,
    LAST_NAME AS LAST_NAME,
    CUSTOMER_ID AS CUSTOMER_ID
  
  FROM Reformat_1_2 AS stg_customers

),

Reformat_1 AS (

  SELECT 
    CUSTOMER_ID AS CUSTOMER_ID0,
    FIRST_NAME AS FIRST_NAME0,
    LAST_NAME AS LAST_NAME0,
    CUSTOMER_ID AS CUSTOMER_ID1,
    FIRST_NAME AS FIRST_NAME1,
    LAST_NAME AS LAST_NAME1,
    CUSTOMER_ID AS CUSTOMER_ID2,
    FIRST_NAME AS FIRST_NAME2,
    LAST_NAME AS LAST_NAME2,
    CUSTOMER_ID AS CUSTOMER_ID3,
    FIRST_NAME AS FIRST_NAME3,
    LAST_NAME AS LAST_NAME3,
    CUSTOMER_ID AS CUSTOMER_ID4,
    FIRST_NAME AS FIRST_NAME4,
    LAST_NAME AS LAST_NAME4,
    CUSTOMER_ID AS CUSTOMER_ID5,
    FIRST_NAME AS FIRST_NAME5,
    LAST_NAME AS LAST_NAME5,
    CUSTOMER_ID AS CUSTOMER_ID6,
    FIRST_NAME AS FIRST_NAME6,
    LAST_NAME AS LAST_NAME6,
    CUSTOMER_ID AS CUSTOMER_ID7,
    FIRST_NAME AS FIRST_NAME7,
    LAST_NAME AS LAST_NAME7,
    CUSTOMER_ID AS CUSTOMER_ID8,
    FIRST_NAME AS FIRST_NAME8,
    LAST_NAME AS LAST_NAME8,
    CUSTOMER_ID AS CUSTOMER_ID9,
    FIRST_NAME AS FIRST_NAME9,
    LAST_NAME AS LAST_NAME9,
    CUSTOMER_ID AS CUSTOMER_ID10,
    FIRST_NAME AS FIRST_NAME10,
    LAST_NAME AS LAST_NAME10,
    CUSTOMER_ID AS CUSTOMER_ID11,
    FIRST_NAME AS FIRST_NAME11,
    LAST_NAME AS LAST_NAME11,
    CUSTOMER_ID AS CUSTOMER_ID12,
    FIRST_NAME AS FIRST_NAME12,
    LAST_NAME AS LAST_NAME12,
    CUSTOMER_ID AS CUSTOMER_ID13,
    FIRST_NAME AS FIRST_NAME13,
    LAST_NAME AS LAST_NAME13,
    CUSTOMER_ID AS CUSTOMER_ID14,
    FIRST_NAME AS FIRST_NAME14,
    LAST_NAME AS LAST_NAME14,
    CUSTOMER_ID AS CUSTOMER_ID15,
    FIRST_NAME AS FIRST_NAME15,
    LAST_NAME AS LAST_NAME15,
    CUSTOMER_ID AS CUSTOMER_ID16,
    FIRST_NAME AS FIRST_NAME16,
    LAST_NAME AS LAST_NAME16,
    CUSTOMER_ID AS CUSTOMER_ID17,
    FIRST_NAME AS FIRST_NAME17,
    LAST_NAME AS LAST_NAME17,
    CUSTOMER_ID AS CUSTOMER_ID18,
    FIRST_NAME AS FIRST_NAME18,
    LAST_NAME AS LAST_NAME18,
    CUSTOMER_ID AS CUSTOMER_ID19,
    FIRST_NAME AS FIRST_NAME19,
    LAST_NAME AS LAST_NAME19,
    FIRST_NAME AS FIRST_NAME,
    LAST_NAME AS LAST_NAME,
    CUSTOMER_ID AS CUSTOMER_ID
  
  FROM stg_customers

),

Reformat_1_1 AS (

  SELECT 
    CUSTOMER_ID AS CUSTOMER_ID0,
    FIRST_NAME AS FIRST_NAME0,
    LAST_NAME AS LAST_NAME0,
    CUSTOMER_ID AS CUSTOMER_ID1,
    FIRST_NAME AS FIRST_NAME1,
    LAST_NAME AS LAST_NAME1,
    CUSTOMER_ID AS CUSTOMER_ID2,
    FIRST_NAME AS FIRST_NAME2,
    LAST_NAME AS LAST_NAME2,
    CUSTOMER_ID AS CUSTOMER_ID3,
    FIRST_NAME AS FIRST_NAME3,
    LAST_NAME AS LAST_NAME3,
    CUSTOMER_ID AS CUSTOMER_ID4,
    FIRST_NAME AS FIRST_NAME4,
    LAST_NAME AS LAST_NAME4,
    CUSTOMER_ID AS CUSTOMER_ID5,
    FIRST_NAME AS FIRST_NAME5,
    LAST_NAME AS LAST_NAME5,
    CUSTOMER_ID AS CUSTOMER_ID6,
    FIRST_NAME AS FIRST_NAME6,
    LAST_NAME AS LAST_NAME6,
    CUSTOMER_ID AS CUSTOMER_ID7,
    FIRST_NAME AS FIRST_NAME7,
    LAST_NAME AS LAST_NAME7,
    CUSTOMER_ID AS CUSTOMER_ID8,
    FIRST_NAME AS FIRST_NAME8,
    LAST_NAME AS LAST_NAME8,
    CUSTOMER_ID AS CUSTOMER_ID9,
    FIRST_NAME AS FIRST_NAME9,
    LAST_NAME AS LAST_NAME9,
    CUSTOMER_ID AS CUSTOMER_ID10,
    FIRST_NAME AS FIRST_NAME10,
    LAST_NAME AS LAST_NAME10,
    CUSTOMER_ID AS CUSTOMER_ID11,
    FIRST_NAME AS FIRST_NAME11,
    LAST_NAME AS LAST_NAME11,
    CUSTOMER_ID AS CUSTOMER_ID12,
    FIRST_NAME AS FIRST_NAME12,
    LAST_NAME AS LAST_NAME12,
    CUSTOMER_ID AS CUSTOMER_ID13,
    FIRST_NAME AS FIRST_NAME13,
    LAST_NAME AS LAST_NAME13,
    CUSTOMER_ID AS CUSTOMER_ID14,
    FIRST_NAME AS FIRST_NAME14,
    LAST_NAME AS LAST_NAME14,
    CUSTOMER_ID AS CUSTOMER_ID15,
    FIRST_NAME AS FIRST_NAME15,
    LAST_NAME AS LAST_NAME15,
    CUSTOMER_ID AS CUSTOMER_ID16,
    FIRST_NAME AS FIRST_NAME16,
    LAST_NAME AS LAST_NAME16,
    CUSTOMER_ID AS CUSTOMER_ID17,
    FIRST_NAME AS FIRST_NAME17,
    LAST_NAME AS LAST_NAME17,
    CUSTOMER_ID AS CUSTOMER_ID18,
    FIRST_NAME AS FIRST_NAME18,
    LAST_NAME AS LAST_NAME18,
    CUSTOMER_ID AS CUSTOMER_ID19,
    FIRST_NAME AS FIRST_NAME19,
    LAST_NAME AS LAST_NAME19,
    FIRST_NAME AS FIRST_NAME,
    LAST_NAME AS LAST_NAME,
    CUSTOMER_ID AS CUSTOMER_ID
  
  FROM Reformat_1 AS stg_customers

),

Reformat_1_3 AS (

  SELECT 
    CUSTOMER_ID AS CUSTOMER_ID0,
    FIRST_NAME AS FIRST_NAME0,
    LAST_NAME AS LAST_NAME0,
    CUSTOMER_ID AS CUSTOMER_ID1,
    FIRST_NAME AS FIRST_NAME1,
    LAST_NAME AS LAST_NAME1,
    CUSTOMER_ID AS CUSTOMER_ID2,
    FIRST_NAME AS FIRST_NAME2,
    LAST_NAME AS LAST_NAME2,
    CUSTOMER_ID AS CUSTOMER_ID3,
    FIRST_NAME AS FIRST_NAME3,
    LAST_NAME AS LAST_NAME3,
    CUSTOMER_ID AS CUSTOMER_ID4,
    FIRST_NAME AS FIRST_NAME4,
    LAST_NAME AS LAST_NAME4,
    CUSTOMER_ID AS CUSTOMER_ID5,
    FIRST_NAME AS FIRST_NAME5,
    LAST_NAME AS LAST_NAME5,
    CUSTOMER_ID AS CUSTOMER_ID6,
    FIRST_NAME AS FIRST_NAME6,
    LAST_NAME AS LAST_NAME6,
    CUSTOMER_ID AS CUSTOMER_ID7,
    FIRST_NAME AS FIRST_NAME7,
    LAST_NAME AS LAST_NAME7,
    CUSTOMER_ID AS CUSTOMER_ID8,
    FIRST_NAME AS FIRST_NAME8,
    LAST_NAME AS LAST_NAME8,
    CUSTOMER_ID AS CUSTOMER_ID9,
    FIRST_NAME AS FIRST_NAME9,
    LAST_NAME AS LAST_NAME9,
    CUSTOMER_ID AS CUSTOMER_ID10,
    FIRST_NAME AS FIRST_NAME10,
    LAST_NAME AS LAST_NAME10,
    CUSTOMER_ID AS CUSTOMER_ID11,
    FIRST_NAME AS FIRST_NAME11,
    LAST_NAME AS LAST_NAME11,
    CUSTOMER_ID AS CUSTOMER_ID12,
    FIRST_NAME AS FIRST_NAME12,
    LAST_NAME AS LAST_NAME12,
    CUSTOMER_ID AS CUSTOMER_ID13,
    FIRST_NAME AS FIRST_NAME13,
    LAST_NAME AS LAST_NAME13,
    CUSTOMER_ID AS CUSTOMER_ID14,
    FIRST_NAME AS FIRST_NAME14,
    LAST_NAME AS LAST_NAME14,
    CUSTOMER_ID AS CUSTOMER_ID15,
    FIRST_NAME AS FIRST_NAME15,
    LAST_NAME AS LAST_NAME15,
    CUSTOMER_ID AS CUSTOMER_ID16,
    FIRST_NAME AS FIRST_NAME16,
    LAST_NAME AS LAST_NAME16,
    CUSTOMER_ID AS CUSTOMER_ID17,
    FIRST_NAME AS FIRST_NAME17,
    LAST_NAME AS LAST_NAME17,
    CUSTOMER_ID AS CUSTOMER_ID18,
    FIRST_NAME AS FIRST_NAME18,
    LAST_NAME AS LAST_NAME18,
    CUSTOMER_ID AS CUSTOMER_ID19,
    FIRST_NAME AS FIRST_NAME19,
    LAST_NAME AS LAST_NAME19,
    FIRST_NAME AS FIRST_NAME,
    LAST_NAME AS LAST_NAME,
    CUSTOMER_ID AS CUSTOMER_ID
  
  FROM stg_customers

),

Reformat_3 AS (

  SELECT 
    CUSTOMER_ID0 AS CUSTOMER_ID0,
    FIRST_NAME0 AS FIRST_NAME0,
    LAST_NAME0 AS LAST_NAME0
  
  FROM Reformat_1_1 AS Reformat_1

),

Reformat_4 AS (

  SELECT 
    FIRST_NAME0 AS FIRST_NAME0,
    LAST_NAME0 AS LAST_NAME0
  
  FROM Reformat_3

),

conc_output AS (

  SELECT concat(FIRST_NAME0, LAST_NAME0) AS full_name
  
  FROM Reformat_4

),

Reformat_1_1_1_1 AS (

  SELECT 
    CUSTOMER_ID AS CUSTOMER_ID0,
    FIRST_NAME AS FIRST_NAME0,
    LAST_NAME AS LAST_NAME0,
    CUSTOMER_ID AS CUSTOMER_ID1,
    FIRST_NAME AS FIRST_NAME1,
    LAST_NAME AS LAST_NAME1,
    CUSTOMER_ID AS CUSTOMER_ID2,
    FIRST_NAME AS FIRST_NAME2,
    LAST_NAME AS LAST_NAME2,
    CUSTOMER_ID AS CUSTOMER_ID3,
    FIRST_NAME AS FIRST_NAME3,
    LAST_NAME AS LAST_NAME3,
    CUSTOMER_ID AS CUSTOMER_ID4,
    FIRST_NAME AS FIRST_NAME4,
    LAST_NAME AS LAST_NAME4,
    CUSTOMER_ID AS CUSTOMER_ID5,
    FIRST_NAME AS FIRST_NAME5,
    LAST_NAME AS LAST_NAME5,
    CUSTOMER_ID AS CUSTOMER_ID6,
    FIRST_NAME AS FIRST_NAME6,
    LAST_NAME AS LAST_NAME6,
    CUSTOMER_ID AS CUSTOMER_ID7,
    FIRST_NAME AS FIRST_NAME7,
    LAST_NAME AS LAST_NAME7,
    CUSTOMER_ID AS CUSTOMER_ID8,
    FIRST_NAME AS FIRST_NAME8,
    LAST_NAME AS LAST_NAME8,
    CUSTOMER_ID AS CUSTOMER_ID9,
    FIRST_NAME AS FIRST_NAME9,
    LAST_NAME AS LAST_NAME9,
    CUSTOMER_ID AS CUSTOMER_ID10,
    FIRST_NAME AS FIRST_NAME10,
    LAST_NAME AS LAST_NAME10,
    CUSTOMER_ID AS CUSTOMER_ID11,
    FIRST_NAME AS FIRST_NAME11,
    LAST_NAME AS LAST_NAME11,
    CUSTOMER_ID AS CUSTOMER_ID12,
    FIRST_NAME AS FIRST_NAME12,
    LAST_NAME AS LAST_NAME12,
    CUSTOMER_ID AS CUSTOMER_ID13,
    FIRST_NAME AS FIRST_NAME13,
    LAST_NAME AS LAST_NAME13,
    CUSTOMER_ID AS CUSTOMER_ID14,
    FIRST_NAME AS FIRST_NAME14,
    LAST_NAME AS LAST_NAME14,
    CUSTOMER_ID AS CUSTOMER_ID15,
    FIRST_NAME AS FIRST_NAME15,
    LAST_NAME AS LAST_NAME15,
    CUSTOMER_ID AS CUSTOMER_ID16,
    FIRST_NAME AS FIRST_NAME16,
    LAST_NAME AS LAST_NAME16,
    CUSTOMER_ID AS CUSTOMER_ID17,
    FIRST_NAME AS FIRST_NAME17,
    LAST_NAME AS LAST_NAME17,
    CUSTOMER_ID AS CUSTOMER_ID18,
    FIRST_NAME AS FIRST_NAME18,
    LAST_NAME AS LAST_NAME18,
    CUSTOMER_ID AS CUSTOMER_ID19,
    FIRST_NAME AS FIRST_NAME19,
    LAST_NAME AS LAST_NAME19,
    FIRST_NAME AS FIRST_NAME,
    LAST_NAME AS LAST_NAME,
    CUSTOMER_ID AS CUSTOMER_ID
  
  FROM Reformat_1_1_1 AS stg_customers

),

Reformat_1_1_2 AS (

  SELECT 
    CUSTOMER_ID AS CUSTOMER_ID0,
    FIRST_NAME AS FIRST_NAME0,
    LAST_NAME AS LAST_NAME0,
    CUSTOMER_ID AS CUSTOMER_ID1,
    FIRST_NAME AS FIRST_NAME1,
    LAST_NAME AS LAST_NAME1,
    CUSTOMER_ID AS CUSTOMER_ID2,
    FIRST_NAME AS FIRST_NAME2,
    LAST_NAME AS LAST_NAME2,
    CUSTOMER_ID AS CUSTOMER_ID3,
    FIRST_NAME AS FIRST_NAME3,
    LAST_NAME AS LAST_NAME3,
    CUSTOMER_ID AS CUSTOMER_ID4,
    FIRST_NAME AS FIRST_NAME4,
    LAST_NAME AS LAST_NAME4,
    CUSTOMER_ID AS CUSTOMER_ID5,
    FIRST_NAME AS FIRST_NAME5,
    LAST_NAME AS LAST_NAME5,
    CUSTOMER_ID AS CUSTOMER_ID6,
    FIRST_NAME AS FIRST_NAME6,
    LAST_NAME AS LAST_NAME6,
    CUSTOMER_ID AS CUSTOMER_ID7,
    FIRST_NAME AS FIRST_NAME7,
    LAST_NAME AS LAST_NAME7,
    CUSTOMER_ID AS CUSTOMER_ID8,
    FIRST_NAME AS FIRST_NAME8,
    LAST_NAME AS LAST_NAME8,
    CUSTOMER_ID AS CUSTOMER_ID9,
    FIRST_NAME AS FIRST_NAME9,
    LAST_NAME AS LAST_NAME9,
    CUSTOMER_ID AS CUSTOMER_ID10,
    FIRST_NAME AS FIRST_NAME10,
    LAST_NAME AS LAST_NAME10,
    CUSTOMER_ID AS CUSTOMER_ID11,
    FIRST_NAME AS FIRST_NAME11,
    LAST_NAME AS LAST_NAME11,
    CUSTOMER_ID AS CUSTOMER_ID12,
    FIRST_NAME AS FIRST_NAME12,
    LAST_NAME AS LAST_NAME12,
    CUSTOMER_ID AS CUSTOMER_ID13,
    FIRST_NAME AS FIRST_NAME13,
    LAST_NAME AS LAST_NAME13,
    CUSTOMER_ID AS CUSTOMER_ID14,
    FIRST_NAME AS FIRST_NAME14,
    LAST_NAME AS LAST_NAME14,
    CUSTOMER_ID AS CUSTOMER_ID15,
    FIRST_NAME AS FIRST_NAME15,
    LAST_NAME AS LAST_NAME15,
    CUSTOMER_ID AS CUSTOMER_ID16,
    FIRST_NAME AS FIRST_NAME16,
    LAST_NAME AS LAST_NAME16,
    CUSTOMER_ID AS CUSTOMER_ID17,
    FIRST_NAME AS FIRST_NAME17,
    LAST_NAME AS LAST_NAME17,
    CUSTOMER_ID AS CUSTOMER_ID18,
    FIRST_NAME AS FIRST_NAME18,
    LAST_NAME AS LAST_NAME18,
    CUSTOMER_ID AS CUSTOMER_ID19,
    FIRST_NAME AS FIRST_NAME19,
    LAST_NAME AS LAST_NAME19,
    FIRST_NAME AS FIRST_NAME,
    LAST_NAME AS LAST_NAME,
    CUSTOMER_ID AS CUSTOMER_ID
  
  FROM Reformat_1_3 AS stg_customers

)

SELECT *

FROM conc_output
