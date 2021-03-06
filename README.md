# rainfall_analysis
A summary assignment for Big Data course

In this project we have 3 Java files whice implements 3 <b>MapReduce</b> process.
<h3>
<li><b>rainfall_mnx_process.java</b> </li> </h3>
In this process we computing the minimum and maximum values:
<ol>
  <li>Month and year in which the largest monthly rainfall fell amount</li>
  <li>Month and year in which the smallest monthly rainfall fell amount</li>
  <li>The year when the largest annual rainfall fell amount</li>
  <li>The year when the smallest annual rainfall fell amount</li>
</ol>

The <b>Map</b> function create pairs like this: <b> <year, Monthly amount> </b> <br>
In this process we find the minimum and maximum values. <br>
In the <b>CleanUp</b> function of the <b>Map process</b>, we print the largest and smallest monthly rainfall fell amount.
<br>
The <b>Reduce</b> function make aggregation and we get pairs in this shape: <strong><year. Annual amount></strong> <br>
Then we find the maximum and minimum year with the <b>CleanUp</b> function of the <b>Reduce process</b>
<h3>
<li><b>rainfall_seasons_process.java</b> </li> </h3>
In this process we calculate the period in the year in which the largest / smallest precipitation fell, a period defined as a season of three consecutive months (winter, fall, summer, spring)<br>
The <b> Map </b> function create the seasons according to the appropriate months and attach to each pair the appropriate season, thus creating pairs of the form: <b> < Amount, Year - Season > </b> , For example: <213, 2018-Winter>
<br>
At the <b> Reduce </b> function we sum the amount of each season each year and create pairs of the form: <b> < Total Seasonal Amount, Year-Season > </b> <br>
Then we find the maximum / minimum season and the year, we show the result obtained with the <b>CleanUp</b> function 
<br> <br> <h3>
<li><b>rainfall_drought_process.java</b> </li> </h3>
 In this process we calculate the multi-annual average as well as the years of drought - that is, when there are at least 3 consecutive years in which rainfall is less than the multi-annual average.<br> <br>
For calculating the multi-annual average, the <b> Map </b> function creates pairs of the form <b> < annual-year average, "drought" > </b>
  <br> The <b> Reduce </b> process sum up all the averages and calculating the multi-annual average.
<br><br>
For calculating the drought period we create pairs of the form: <b> < < A list of average-year>, "drought" > </b> in the <b> Reduce </b> process
We used <b> TreeMap </b> that create the pairs hierarchically (sorted by year). <br>
  Using the <b> TreeMap </b> we identify periods of drought, every time we identify drought we show it in the form:
  <b> < Total average of the drought period, the drought period (end year - start year) > </b>
