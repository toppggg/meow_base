import os
# Setup parameters
num = 1000
infile = 'somehere\particular'
outfile = 'nowhere\particular'

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