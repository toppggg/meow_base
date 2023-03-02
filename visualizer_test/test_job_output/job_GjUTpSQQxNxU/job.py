import os

# Setup parameters

num = 10000
infile = 'test_monitor_base\\start\\A.txt'
outfile = 'test_monitor_base\\output\\A.txt'


with open(infile, 'r') as file:

    s = float(file.read())

for i in range(num):

    s += i



div_by = 4

result = s / div_by



print(result)



os.makedirs(os.path.dirname(outfile), exist_ok=True)



with open(outfile, 'w') as file:

    file.write(str(result))



print('done')