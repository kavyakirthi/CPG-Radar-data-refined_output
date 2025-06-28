# CPG-Radar-data-refined_output

## 1.Dynamic Reception
The "category" field in the "reception" object is now populated dynamically based on a difference in scores as follows:

"Above Average" if the difference is > 0.5
"Below Average" if the difference is < -0.5
else "Average" 

## 2.Dynamic Split Logic
I had added the split cast and writer names as part of the logic, will now validate that both fields are filled in and that the same number of counts in both fields. This will allow a row to be completely skipped with warning messages recorded anytime the data are potentially not clean. 

## 3.Row-Level Errors
I kept all of the logic around the processing of movies are contained within a try / except block. If there are any issues while processing the movie in a row the error messages, and if possible the movie id, will be recorded and processing will continue with the next row without stopping the overall script's operations.

## 4.Logging
All print() statements were upgraded to use Python's logging module and to be consistent with better intent, included logging levels (info, warning, error). 

## 5.Parameterized File Paths
File input and output paths will not be hard-coded, but passed into the script or main function as a parameter instead. 
