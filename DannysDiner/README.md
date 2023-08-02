# Problem Statement

Danny wants to use the data to answer a few simple questions about his customers, especially about their visiting patterns, how much money they’ve spent and also which menu items are their favourite. Having this deeper connection with his customers will help him deliver a better and more personalised experience for his loyal customers.

He plans on using these insights to help him decide whether he should expand the existing customer loyalty program - additionally he needs help to generate some basic datasets so his team can easily inspect the data without needing to use SQL.

Danny has provided you with a sample of his overall customer data due to privacy issues - but he hopes that these examples are enough for you to write fully functioning SQL queries to help him answer his questions!

Danny has shared with you 3 key datasets for this case study:

sales
menu
members
You can inspect the entity relationship diagram and example data below.

![ERDiagram.png](attachment:ERDiagram.png)


```python
import mysql.connector
from mysql.connector import errorcode

from utils import read_config, create_database, dataframe_query 

DB_NAME = 'dannys_diner'

config = read_config(config_file = "../config.ini")

cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()
```


```python
cursor.execute("USE {}".format(DB_NAME))
```

1. What is the total amount each customer spent at the restaurant?


```python
query1 = ("SELECT \
          S.customer_id as customer, \
          sum(M.price) as TotalSpent \
          FROM sales S, menu M \
          WHERE S.product_id = M.product_id \
          GROUP BY S.customer_id")

dataframe_query(cnx, query1)[1]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer</th>
      <th>TotalSpent</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>76</td>
    </tr>
    <tr>
      <th>1</th>
      <td>B</td>
      <td>74</td>
    </tr>
    <tr>
      <th>2</th>
      <td>C</td>
      <td>36</td>
    </tr>
  </tbody>
</table>
</div>



2. How many days has each customer visited the restaurant?


```python
query2 = ("SELECT customer_id as Customer, \
           COUNT(DISTINCT(order_date)) as Vists \
           FROM sales \
           GROUP BY customer_id")

dataframe_query(cnx, query2)[1]

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Customer</th>
      <th>Vists</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>4</td>
    </tr>
    <tr>
      <th>1</th>
      <td>B</td>
      <td>6</td>
    </tr>
    <tr>
      <th>2</th>
      <td>C</td>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>



3. What was the first item from the menu purchased by each customer?


```python
query3 = ("SELECT Customer, Product \
           FROM (SELECT \
                 S.customer_id AS Customer, \
                 S.product_id, \
                 M.product_name AS Product, \
                 ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS rnk \
                 FROM sales S \
                 INNER JOIN menu M \
                    ON S.product_id = M.product_id) ranked_sales \
            WHERE rnk = 1")

dataframe_query(cnx, query3)[1]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Customer</th>
      <th>Product</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>sushi</td>
    </tr>
    <tr>
      <th>1</th>
      <td>B</td>
      <td>curry</td>
    </tr>
    <tr>
      <th>2</th>
      <td>C</td>
      <td>ramen</td>
    </tr>
  </tbody>
</table>
</div>



4. What is the most purchased item on the menu and how many times was it purchased by all customers?


```python
query4 = ("SELECT S.customer_id AS Customer, \
           M.product_name AS Product, \
           count(S.customer_id) AS Frequency\
           FROM sales S, menu M \
           WHERE S.product_id = (SELECT product_id \
                                 FROM sales \
                                 GROUP BY product_id \
                                 ORDER BY COUNT(product_id) DESC LIMIT 1) \
           AND S.product_id = M.product_id \
           GROUP BY S.customer_id, M.product_name")

dataframe_query(cnx, query4)[1]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Customer</th>
      <th>Product</th>
      <th>Frequency</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>ramen</td>
      <td>3</td>
    </tr>
    <tr>
      <th>1</th>
      <td>B</td>
      <td>ramen</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2</th>
      <td>C</td>
      <td>ramen</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
</div>



5. Which item was the most popular for each customer?


```python
query5 = ("SELECT Customer, MostPopular \
          FROM(SELECT \
                S.customer_id AS Customer, \
                M.product_name As MostPopular, \
               DENSE_RANK() OVER (PARTITION BY S.customer_id ORDER BY count(S.product_id) DESC) AS rnk \
                FROM sales S \
                    INNER JOIN menu M \
                    ON S.product_id = M.product_id \
                    GROUP BY S.customer_id, M.product_name) MP \
          WHERE rnk = 1")

dataframe_query(cnx, query5)[1]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Customer</th>
      <th>MostPopular</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>ramen</td>
    </tr>
    <tr>
      <th>1</th>
      <td>B</td>
      <td>curry</td>
    </tr>
    <tr>
      <th>2</th>
      <td>B</td>
      <td>sushi</td>
    </tr>
    <tr>
      <th>3</th>
      <td>B</td>
      <td>ramen</td>
    </tr>
    <tr>
      <th>4</th>
      <td>C</td>
      <td>ramen</td>
    </tr>
  </tbody>
</table>
</div>



6. Which item was purchased first by the customer after they became a member?


```python
query6 = ("SELECT Customer, FirstItem \
            FROM (SELECT S.customer_id AS Customer, \
                    Me.product_name AS FirstItem, \
                    RANK() OVER(PARTITION BY S.customer_id ORDER BY S.order_date) AS rnk \
                    FROM sales S \
                    INNER JOIN members M \
                    ON S.customer_id = M.customer_id \
                    INNER JOIN Menu Me \
                    ON S.product_id = Me.product_id \
                    WHERE S.order_date >= M.join_date) FI \
            WHERE rnk = 1")

dataframe_query(cnx, query6)[1]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Customer</th>
      <th>FirstItem</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>curry</td>
    </tr>
    <tr>
      <th>1</th>
      <td>B</td>
      <td>sushi</td>
    </tr>
  </tbody>
</table>
</div>



7. Which item was purchased just before the customer became a member?


```python
query7 = ("SELECT Customer, LastItem \
            FROM (SELECT S.customer_id AS Customer, \
                    Me.product_name AS LastItem, \
                    RANK() OVER(PARTITION BY S.customer_id ORDER BY S.order_date DESC) AS rnk \
                    FROM sales S \
                    INNER JOIN members M \
                    ON S.customer_id = M.customer_id \
                    INNER JOIN Menu Me \
                    ON S.product_id = Me.product_id \
                    WHERE S.order_date < M.join_date) FI \
            WHERE rnk = 1")

dataframe_query(cnx, query7)[1]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Customer</th>
      <th>LastItem</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>sushi</td>
    </tr>
    <tr>
      <th>1</th>
      <td>A</td>
      <td>curry</td>
    </tr>
    <tr>
      <th>2</th>
      <td>B</td>
      <td>sushi</td>
    </tr>
  </tbody>
</table>
</div>



8. What is the total items and amount spent for each member before they became a member?


```python
query8 = ("SELECT S.customer_id, \
          COUNT(S.product_id) AS TotalOrders, \
          SUM(ME.price) as TotalSpent \
          FROM sales S \
          INNER JOIN members M \
          ON S.customer_id = M.customer_id \
          INNER JOIN menu ME \
          ON S.product_id = ME.product_id \
          WHERE S.order_date < M.join_date \
          GROUP BY S.customer_id \
          ORDER BY S.customer_id")

dataframe_query(cnx, query8)[1]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>TotalOrders</th>
      <th>TotalSpent</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>2</td>
      <td>25</td>
    </tr>
    <tr>
      <th>1</th>
      <td>B</td>
      <td>3</td>
      <td>40</td>
    </tr>
  </tbody>
</table>
</div>



9. If each $1 spent equates to 10 points and sushi has a 2x points multiplier - how many points would each customer have?


```python
query9 = ("SELECT Customer, SUM(PTS) \
           FROM (SELECT S.customer_id AS Customer, \
           CASE \
            WHEN S.product_id = 1 THEN M.price * 20 \
            ELSE M.price * 10 END \
           AS PTS \
           FROM sales S \
           INNER JOIN menu M \
           ON S.product_id = M.product_id) PTS_TABLE \
           GROUP BY Customer")

dataframe_query(cnx, query9)[1]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Customer</th>
      <th>SUM(PTS)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>860</td>
    </tr>
    <tr>
      <th>1</th>
      <td>B</td>
      <td>940</td>
    </tr>
    <tr>
      <th>2</th>
      <td>C</td>
      <td>360</td>
    </tr>
  </tbody>
</table>
</div>



10. In the first week after a customer joins the program (including their join date) they earn 2x points on all items, not just sushi - how many points do customer A and B have at the end of January?



```python
query10 = (f"SELECT Customer, SUM(PTS) as TotalPTS \
            FROM (SELECT S.customer_id AS Customer, \
            CASE \
                WHEN S.order_date < M.join_date \
                    AND S.product_id = 1 THEN Me.price * 20 \
                WHEN S.order_date >= M.join_date \
                    AND S.order_date < M.join_date + 7 THEN Me.price * 20 \
                ELSE Me.price * 10 \
            END AS PTS \
            FROM sales S \
            INNER JOIN members M \
            ON S.customer_id = M.customer_id \
            INNER JOIN menu Me \
            ON S.product_id = Me.product_id \
            WHERE S.order_date < '2021-01-31' \
            ORDER BY S.customer_id, S.order_date) MemberPTS \
            GROUP BY Customer")

dataframe_query(cnx, query10)[1]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Customer</th>
      <th>TotalPTS</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>B</td>
      <td>820</td>
    </tr>
    <tr>
      <th>1</th>
      <td>A</td>
      <td>1370</td>
    </tr>
  </tbody>
</table>
</div>



### Bonus Questions
#### Join All The Things
1. The following questions are related creating basic data tables that Danny and his team can use to quickly derive insights without needing to join the underlying tables using SQL.

Recreate the following table output using the available data:

| customer_id |	order_date | product_name | price | member |
|-------------|:----------:|:------------:|:-----:|-------:|
| A | 2021-01-01 | curry | 15 | N |
| A | 2021-01-01 | sushi | 10 | N |
| A | 2021-01-07 | curry | 15 | Y |
| A | 2021-01-10 | ramen | 12 | Y |
| A | 2021-01-11 | ramen | 12 | Y |
| A | 2021-01-11 | ramen | 12 | Y |
| B | 2021-01-01 | curry | 15 | N |
| B | 2021-01-02 | curry | 15 | N |
| B | 2021-01-04 | sushi | 10 | N |
| B | 2021-01-11 | sushi | 10 | Y |
| B | 2021-01-16 | ramen | 12 | Y |
| B | 2021-02-01 | ramen | 12 | Y |
| C | 2021-01-01 | ramen | 12 | N |
| C | 2021-01-01 | ramen | 12 | N |
| C | 2021-01-07 | ramen | 12 | N |



```python
bonus1 = (f"SELECT S.customer_id, S.order_date, \
             Me.product_name, Me.price, \
             CASE \
                WHEN S.order_date >= M.join_date THEN 'Y' \
                ELSE 'N' \
             END AS member \
             FROM sales S \
             INNER JOIN menu Me \
             ON S.product_id = Me.product_id \
             LEFT JOIN members M \
             ON S.customer_id = M.customer_id \
             ORDER BY S.customer_id, S.order_date, Me.product_name")

dataframe_query(cnx, bonus1)[1]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>order_date</th>
      <th>product_name</th>
      <th>price</th>
      <th>member</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>2021-01-01</td>
      <td>curry</td>
      <td>15</td>
      <td>N</td>
    </tr>
    <tr>
      <th>1</th>
      <td>A</td>
      <td>2021-01-01</td>
      <td>sushi</td>
      <td>10</td>
      <td>N</td>
    </tr>
    <tr>
      <th>2</th>
      <td>A</td>
      <td>2021-01-07</td>
      <td>curry</td>
      <td>15</td>
      <td>Y</td>
    </tr>
    <tr>
      <th>3</th>
      <td>A</td>
      <td>2021-01-10</td>
      <td>ramen</td>
      <td>12</td>
      <td>Y</td>
    </tr>
    <tr>
      <th>4</th>
      <td>A</td>
      <td>2021-01-11</td>
      <td>ramen</td>
      <td>12</td>
      <td>Y</td>
    </tr>
    <tr>
      <th>5</th>
      <td>A</td>
      <td>2021-01-11</td>
      <td>ramen</td>
      <td>12</td>
      <td>Y</td>
    </tr>
    <tr>
      <th>6</th>
      <td>B</td>
      <td>2021-01-01</td>
      <td>curry</td>
      <td>15</td>
      <td>N</td>
    </tr>
    <tr>
      <th>7</th>
      <td>B</td>
      <td>2021-01-02</td>
      <td>curry</td>
      <td>15</td>
      <td>N</td>
    </tr>
    <tr>
      <th>8</th>
      <td>B</td>
      <td>2021-01-04</td>
      <td>sushi</td>
      <td>10</td>
      <td>N</td>
    </tr>
    <tr>
      <th>9</th>
      <td>B</td>
      <td>2021-01-11</td>
      <td>sushi</td>
      <td>10</td>
      <td>Y</td>
    </tr>
    <tr>
      <th>10</th>
      <td>B</td>
      <td>2021-01-16</td>
      <td>ramen</td>
      <td>12</td>
      <td>Y</td>
    </tr>
    <tr>
      <th>11</th>
      <td>B</td>
      <td>2021-02-01</td>
      <td>ramen</td>
      <td>12</td>
      <td>Y</td>
    </tr>
    <tr>
      <th>12</th>
      <td>C</td>
      <td>2021-01-01</td>
      <td>ramen</td>
      <td>12</td>
      <td>N</td>
    </tr>
    <tr>
      <th>13</th>
      <td>C</td>
      <td>2021-01-01</td>
      <td>ramen</td>
      <td>12</td>
      <td>N</td>
    </tr>
    <tr>
      <th>14</th>
      <td>C</td>
      <td>2021-01-07</td>
      <td>ramen</td>
      <td>12</td>
      <td>N</td>
    </tr>
  </tbody>
</table>
</div>



2. Rank All The Things
Danny also requires further information about the ranking of customer products, but he purposely does not need the ranking for non-member purchases so he expects null ranking values for the records when customers are not yet part of the loyalty program.

| customer_id | order_date | product_name | price | member | ranking |
|-------------|:----------:|:------------:|:-----:|:------:|--------:|
A | 2021-01-01 | curry | 15 | N | null |
A | 2021-01-01 | sushi | 10 | N | null |
A | 2021-01-07 | curry | 15 | Y | 1 |
A | 2021-01-10 | ramen | 12 | Y | 2 |
A | 2021-01-11 | ramen | 12 | Y | 3 |
A | 2021-01-11 | ramen | 12 | Y | 3 |
B | 2021-01-01 | curry | 15 | N | null |
B | 2021-01-02 | curry | 15 | N | null |
B | 2021-01-04 | sushi | 10 | N | null |
B | 2021-01-11 | sushi | 10 | Y | 1 |
B | 2021-01-16 | ramen | 12 | Y | 2 |
B | 2021-02-01 | ramen | 12 | Y | 3 |
C | 2021-01-01 | ramen | 12 | N | null |
C | 2021-01-01 | ramen | 12 | N | null |
C | 2021-01-07 | ramen | 12 | N | null |


```python
bonus2 = (f"SELECT *, \
            CASE \
                WHEN member = 'N' THEN null \
                ELSE RANK() OVER(PARTITION BY customer_id, member ORDER BY order_date) \
            END AS ranking \
            FROM (SELECT S.customer_id, S.order_date, Me.product_name, Me.price,  \
                  CASE \
                    WHEN S.order_date >= M.join_date THEN 'Y' \
                    ELSE 'N' \
                  END AS member \
                  FROM sales S \
                  INNER JOIN menu Me \
                  ON S.product_id = Me.product_id \
                  LEFT JOIN members M \
                  ON S.customer_id = M.customer_id \
                  ORDER BY S.customer_id, S.order_date, Me.product_name) Rankings")

dataframe_query(cnx, bonus2)[1]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>order_date</th>
      <th>product_name</th>
      <th>price</th>
      <th>member</th>
      <th>ranking</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>2021-01-01</td>
      <td>curry</td>
      <td>15</td>
      <td>N</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>A</td>
      <td>2021-01-01</td>
      <td>sushi</td>
      <td>10</td>
      <td>N</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>A</td>
      <td>2021-01-07</td>
      <td>curry</td>
      <td>15</td>
      <td>Y</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>A</td>
      <td>2021-01-10</td>
      <td>ramen</td>
      <td>12</td>
      <td>Y</td>
      <td>2.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>A</td>
      <td>2021-01-11</td>
      <td>ramen</td>
      <td>12</td>
      <td>Y</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>A</td>
      <td>2021-01-11</td>
      <td>ramen</td>
      <td>12</td>
      <td>Y</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>B</td>
      <td>2021-01-01</td>
      <td>curry</td>
      <td>15</td>
      <td>N</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>7</th>
      <td>B</td>
      <td>2021-01-02</td>
      <td>curry</td>
      <td>15</td>
      <td>N</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>8</th>
      <td>B</td>
      <td>2021-01-04</td>
      <td>sushi</td>
      <td>10</td>
      <td>N</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>9</th>
      <td>B</td>
      <td>2021-01-11</td>
      <td>sushi</td>
      <td>10</td>
      <td>Y</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>10</th>
      <td>B</td>
      <td>2021-01-16</td>
      <td>ramen</td>
      <td>12</td>
      <td>Y</td>
      <td>2.0</td>
    </tr>
    <tr>
      <th>11</th>
      <td>B</td>
      <td>2021-02-01</td>
      <td>ramen</td>
      <td>12</td>
      <td>Y</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>12</th>
      <td>C</td>
      <td>2021-01-01</td>
      <td>ramen</td>
      <td>12</td>
      <td>N</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>13</th>
      <td>C</td>
      <td>2021-01-01</td>
      <td>ramen</td>
      <td>12</td>
      <td>N</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>14</th>
      <td>C</td>
      <td>2021-01-07</td>
      <td>ramen</td>
      <td>12</td>
      <td>N</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>




```python
cursor.close()
cnx.close()
```
