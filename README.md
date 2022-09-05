# data-eng-test
Apache beam tech tasks

## Assumptions

Based on the instructions and details given in the tech task document. I have made the following assumptions.
- I have created my own csv file based on the details given for the ease of testing.
- The csv file is named "test.csv" with the columns - date and transcation_amount. The date format was assumed to be "YYYY-MM-DD" as this was also mentioned in the task document.

## Work done
- I have used the created "test.csv" to develop solution for task 1 and task 2 in google colab as requested in the tech test. Please refer to files "task1.ipynb" and "task2.ipynb".

- Following on, I have changed the input and output location to the one mentioned in the tech test so that it would read the file "transactions.csv" from google storage bucket and save the output into the mentioned output folder with the file name "results.csv". Please refer to files "task1.py" and "task2.py" for this.
