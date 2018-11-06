# rainfall_analysis
A summary assignment from the Big Data course

in this project we have 3 Java files whice implements 3 <strong>MapReduce</strong> process.

<li><b>rainfall_mnx_process.java</b> </li>
in this process we computing the minimum and maximum values:
<ol>
  <li>Month and year in which the largest monthly rainfall fell amount</li>
  <li>Month and year in which the smallest monthly rainfall fell amount</li>
  <li>The year when the largest annual rainfall fell amount</li>
  <li>The year when the smallest annual rainfall fell amount</li>
</ol>

The <strong>Map</strong> function create tuples like this: <strong> <year, Monthly amount> </strong> <br>
in this process we find the minimum and maximum values. <br>
in the <strong>CleanUp</strong> function of the <strong>Map process</strong>, we print the largest and smallest monthly rainfall fell amount.
<br>
The <strong>Reduce</strong> function make aggregation and we get tuples in this shape: <strong><year. annual amount></strong> <br>
Then we find the maximum and minimum year with the <strong>CleanUp</strong> function of the <strong>Reduce process</strong>

<li><b>rainfall_seasons_process.java</b> </li>

<li><b>rainfall_drought_process.java</b> </li>
