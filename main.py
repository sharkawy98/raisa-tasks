from time import time
from task0 import task_0
from task1 import task_1
from task2 import task_2
from task3 import task_3
from task4 import final_task


t1 = time()
task_0()
wells_df = task_1()
wells_df = task_2(wells_df)
wells_df = task_3(wells_df)
wells_df.to_csv('outputs/wells.csv', index=False)
final_task()
t2 = time()
print(('It takes %.2f seconds to run all tasks') % (t2 - t1))
